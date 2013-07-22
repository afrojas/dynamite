module Dynamite
  class << self
    attr_accessor :configuration, :path_to_gem_lib

    def config
      unless self.configuration
        self.configuration = Configuration.new
        self.configuration.config_file_loaded = false
      end
      self.configuration
    end

    def configure
      yield(config)
    end

    def log
      config.log
    end
  end

  class Configuration < Hashie::Mash
    attr_accessor :config_file_loaded

    def public_environment?
      production? || prerelease?
    end
    
    def private_environment?
      !public_environment
    end
    
    def development?
      environment == 'development'
    end

    def staging?
      environment == 'staging'
    end
    
    def prerelease?
      environment == 'prerelease'
    end

    def production?
      environment == 'production'
    end

    def access_key
      config_file_item('access_key')
    end

    def secret_key
      config_file_item('secret_key')
    end

    def endpoint
      config_file_item('dynamo_db')['endpoint']
    end

    def port
      config_file_item('dynamo_db')['port']
    end

    def config_file_item(item)
      load_config_file unless self.config_file_loaded
      self.file[environment][item.to_s]
    end

    def load_config_file
      self.file = YAML.load(ERB.new(File.read(self.path_to_config_file)).result)
      self.config_file_loaded = true
    end
  end
end