
require "kafka_etl_base/version"

require 'poseidon'
require 'zk'
require 'parallel'
require 'logger'

module KafkaETLBase
  class Base
    
    def initialize(zookeeper, kafka_brokers, kafka_topic, opts={})
      
      @num_threads = opts[:num_threads] ? opts[:num_threads] : 2 
      @max_fetch_bytes = opts[:max_fetch_bytes] ? opts[:max_fetch_bytes] : 10_000_000
      @kafka_clint_id = opts[:kafka_client_id] ? opts[:kafka_client_id] : "my_consumer"
      @kafka_part_num = opts[:kafka_topic_part_num] ? opts[:kafka_topic_part_num] : 4
      
      @zookeeper = zookeeper
      @kafka_brokers = kafka_brokers
      @kafka_topic = kafka_topic
      
      $log = Logger.new(STDOUT) if $log.nil?

      $log.debug("zookeeper: #{@zookeeper}")
      $log.debug("kafka_brokers: #{@kafka_brokers}")
      $log.debug("kafka_topic: #{@kafka_topic}")
      
    end
    
    def process()
      
      zk = ZK.new(@zookeeper)
      zk.create("/", ignore: :node_exists)
      
      @total_procs = 0
      begin
        seq = [ * 0 ... @kafka_part_num ].shuffle
        r = Parallel.each(seq, :in_threads => @num_threads) do |part_no|
          zk_lock = "lock_hdfs_part_#{part_no}"
          zk.with_lock(zk_lock) do
            proccess_thread(zk, part_no)
          end
        end
      ensure
        zk.close
      end
      $log.info "total procs: #{@total_procs}"
    end
    
    def proccess_thread(zk, part_no)
      zk_part_node = "/part_offset_#{part_no}"
      
      num_cur_part_procs = 0
      $log.info "\npart #{part_no} start"
      
      if ! zk.exists?(zk_part_node)
        zk.create(zk_part_node, "0")
      end
      
      offset = nil
      begin
        value, stat = zk.get(zk_part_node)
        offset = value.to_i
        if offset == 0
          part_offset = :earliest_offset
        else
          part_offset = offset
        end
      rescue ZK::Exceptions::NoNode => e
        part_offset = :latest_offset
      end
      $log.info "part: #{part_no}, offset: #{part_offset}"
      
      cons = Poseidon::PartitionConsumer.consumer_for_partition(@kafka_client_id,
                                                                @kafka_brokers,
                                                                @kafka_topic,
                                                                part_no,
                                                                part_offset, :max_wait_ms => 100, :max_bytes => @max_fetch_bytes)
      begin
        num_cur_part_procs += process_messages(cons)
        
        next_offset = cons.next_offset
        $log.info "next_offset: #{next_offset} proc: #{num_cur_part_procs}"
        @total_procs += num_cur_part_procs
        
        # set next offset to zookeper
        zk.set(zk_part_node, next_offset.to_s) if next_offset >= offset
      rescue Poseidon::Errors::NotLeaderForPartition => e
        $log.info "Skip: Not Leader For Partition"
      rescue Poseidon::Errors::OffsetOutOfRange => e
        $log.error e.to_s
        k.set(zk_part_node, "0")
      #rescue NoMethodError => e
      #  next_offset = cons.next_offset
      #  zk.set(zk_part_node, next_offset.to_s)
      #  $log.error e.to_s
      rescue => e
        raise e
      ensure
        cons.close if ! cons.nil?
      end
    rescue Poseidon::Errors::OffsetOutOfRange => e
      $log.error e.to_s
      zk.set(zk_part_node, "0")
    end
    
    def process_messages(cons)
      messages = cons.fetch
      messages.each do |m|
        key = m.key
        val = m.value
        puts "key: #{key}, val: #{val}"
      end
    end
  end
end
