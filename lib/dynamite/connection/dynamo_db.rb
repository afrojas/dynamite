module Dynamite
  class DynamoDB
    include ::Dynamite::DynamoDB::API

    class << self
      attr_accessor :write_delay

      def instance
        @instance ||= self.new
      end
    end

    def initialize
      renew_credentials
      self.class.write_delay = 0
    end

    def self.delayed_writes(delay_in_seconds, &blk)
      original_write_delay = self.write_delay
      self.write_delay = delay_in_seconds
      blk.call if blk
      self.write_delay = original_write_delay
    end

  end
end