require 'hashie'
require 'em-synchrony'
require 'em-synchrony/em-http'
require 'nokogiri'
require 'eventmachine'
require 'yaml'
require 'erb'
require 'oj'
require 'yajl'
require 'active_support/core_ext'
require  'msgpack'

require 'dynamite/version'
require 'dynamite/connection/exceptions'
require 'dynamite/config'
require 'dynamite/connection/sts'
require 'dynamite/connection/stats'
require 'dynamite/connection/raw_request'
require 'dynamite/connection/api'
require 'dynamite/connection/dynamo_db'

require 'dynamite/utils/dyna_pack'
require 'dynamite/utils/log'
require 'dynamite/orm/dynamo_key'
require 'dynamite/orm/document'
require 'dynamite/orm/document/callbacks'
require 'dynamite/orm/document/class_methods'
require 'dynamite/orm/document/instance_methods'
require 'dynamite/orm/document/associations'
require 'dynamite/orm/document/polymorphic'
require 'dynamite/orm/document/cacheable'

Dynamite.path_to_gem_lib = File.expand_path(File.dirname(__FILE__))
Dynamite.configure do |config|
  config.path_to_config_file = "#{::Dynamite.path_to_gem_lib}/dynamite_sample.yml"
  config.environment = 'development'
  config.log = ::Dynamite::Log
end

module Dynamite
  def self.async(&block)
    value = nil
    if EventMachine.reactor_running?
      value = yield
    else
      EventMachine::run do
        fiber = Fiber.new do
          value = yield
          EM.stop
        end
        fiber.resume
      end
    end
    value
  end
end