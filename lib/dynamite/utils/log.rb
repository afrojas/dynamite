module Dynamite
  class Log
    def self.instance
      if @log.nil?
        @log = ActiveSupport::BufferedLogger.new(STDOUT)
        # TODO: uncomment this
        $stdout.sync = true #unless Wizards.production?
      end
      @log
    end

    def self.debug(message)
      instance.add(Logger::DEBUG, prefix('DEBUG', message))
    end

    def self.info(message)
      instance.add(Logger::INFO, prefix('INFO', message))
    end

    def self.error(message)
      instance.add(Logger::ERROR, prefix('ERROR', message))
    end

    def self.warn(message)
      instance.add(Logger::WARN, prefix('WARN', message))
    end

    def self.prefix(code, message)
      connection_id = Fiber.current.instance_variable_get(:@connection_id) || "unknown"
      "[#{code}] #{Time.now.strftime('%Y-%m-%d %H:%M:%S')} [#{connection_id}]:: #{message}"
    end
  end
end