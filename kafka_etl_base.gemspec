# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'kafka_etl_base/version'

Gem::Specification.new do |spec|
  spec.name          = "kafka_etl_base"
  spec.version       = KafkaEtlBase::VERSION
  spec.authors       = ["Shinji Ikeda"]
  spec.email         = ["gm.ikeda@gmail.com"]
  spec.summary       = %q{Kafa ETL Base}
  spec.description   = %q{}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  
  spec.add_dependency "poseidon"
  spec.add_dependency "zk"
  spec.add_dependency "parallel"
end
