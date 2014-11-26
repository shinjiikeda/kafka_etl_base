
require 'poseidon'
require 'zk'
require 'parallel'
require 'logger'

module KafkaETLBase
  class BackendError < StandardError; end
  class Base
    def initialize(zookeeper, kafka_brokers, kafka_topic, opts={})
      
      @num_threads = opts[:num_threads] ? opts[:num_threads] : 2 
      @max_fetch_bytes = opts[:max_fetch_bytes] ? opts[:max_fetch_bytes] : 5_000_000
      @min_fetch_bytes = opts[:min_fetch_bytes] ? opts[:min_fetch_bytes] : 0
      @kafka_client_id = opts[:kafka_client_id] ? opts[:kafka_client_id] : "my_consumer"
      @kafka_part_num = opts[:kafka_topic_part_num] ? opts[:kafka_topic_part_num] : 2
      
      @zookeeper = zookeeper
      @kafka_brokers = kafka_brokers
      @kafka_topic = kafka_topic
      
      $log = Logger.new(STDOUT) if $log.nil?

      $log.debug("zookeeper: #{@zookeeper}")
      $log.debug("kafka_brokers: #{@kafka_brokers}")
      $log.debug("kafka_topic: #{@kafka_topic}")
      $log.debug("kafka_client_id: #{@kafka_client_id}")
      
      @partition_shuffle = true
      
      @mutex = Mutex.new
      
      @total_procs = 0
    end
    
    def process()
      
      @total_procs = 0
      
      zk = ZK.new(@zookeeper)
      begin
        #zk.create("/", ignore: :node_exists)
        if zk.exists?("/stop_etl")
          $log.info("zk: /stop_etl exist.")
          $log.info("stop message processing.")
          return
        end
      ensure
        zk.close!
      end
      
      seq = [ * 0 ... @kafka_part_num ]
      seq.shuffle! if @partition_shuffle == true
      
      r = Parallel.each(seq, :in_threads => @num_threads) do |part_no|
        zk_th = nil
        begin
          zk_th = ZK.new(@zookeeper)
          zk_lock = "lock_hdfs_part_#{part_no}"
          locker = zk_th.locker(zk_lock)
          begin
            if locker.lock!
              remain = proccess_thread(zk_th, part_no)
            else
              $log.info("part: #{part_no} is already locked skip")
            end
          ensure
            begin
              locker.unlock!
            rescue => e
              $log.error(e.inspect)
              $log.error(e.backtrace)
            end
          end
        rescue ZK::Exceptions::ConnectionLoss => e
          $log.error(e.inspect)
          $log.error(e.backtrace)
        ensure
          begin
            zk_th.close! if ! zk_th.nil?
          rescue
          end
        end
      end
      $log.info "total procs: #{@total_procs}"
    end
    
    def process_partition(part_no)
      zk = ZK.new(@zookeeper)
      begin
        zk_lock = "lock_hdfs_part_#{part_no}"
        locker = zk.locker(zk_lock)
        begin
          if locker.lock!
            remain = proccess_thread(zk, part_no)
          else
            $log.info("part: #{part_no} is already locked skip")
          end
        ensure
          locker.unlock!
        end
      ensure
        zk.close
      end
    end
    
    def proccess_thread(zk, part_no)
      zk_part_node = "/part_offset_#{part_no}"
      
      num_cur_part_procs = 0
      
      if ! zk.exists?(zk_part_node)
        zk.create(zk_part_node, "0")
      end
      
      offset = nil
      begin
        value, stat = zk.get(zk_part_node)
        offset = value.to_i
        if offset == 0
          part_offset = :earliest_offset
          #part_offset = :latest_offset
        else
          part_offset = offset
        end
      rescue ZK::Exceptions::NoNode => e
        part_offset = :earliest_offset
        #part_offset = :latest_offset
      end
      $log.debug "part: #{part_no}, offset: #{part_offset}"
      
      cons = Poseidon::PartitionConsumer.consumer_for_partition(@kafka_client_id,
                                                                @kafka_brokers,
                                                                @kafka_topic,
                                                                part_no,
                                                                part_offset,
                                                                :max_wait_ms => 0,
                                                                :max_bytes => @max_fetch_bytes,
                                                                :min_bytes => @min_fetch_bytes
                                                               )
      begin
        num_cur_part_procs += process_messages(cons, part_no)
        
        next_offset = cons.next_offset
        last_offset = cons.highwater_mark
        $log.info "part: #{part_no}, next_offset: #{next_offset}, last_offset: #{last_offset},  proc: #{num_cur_part_procs}"
        @mutex.synchronize do
          @total_procs += num_cur_part_procs
        end
        # set next offset to zookeper
        zk.set(zk_part_node, next_offset.to_s) if next_offset >= offset
        return last_offset - next_offset
      rescue Poseidon::Connection::ConnectionFailedError
        $log.error "kafka connection failed"
        return 0
      rescue BackendError
        $log.debug "backend error"
        return 0
      ensure
        cons.close if ! cons.nil?
      end
    rescue Poseidon::Errors::OffsetOutOfRange => e
      zk.set(zk_part_node, "0")
      $log.error e.inspect
      return 0
    rescue Poseidon::Errors::NotLeaderForPartition, Poseidon::Errors::UnableToFetchMetadata => e
      $log.error e.inspect
      return 0
    end
    
    def process_messages(cons, part_no)
      messages = cons.fetch
      messages.each do |m|
        key = m.key
        val = m.value
        puts "part: #{part_no}, key: #{key}, val: #{val}"
      end
      messages.size
    end
  end
end
