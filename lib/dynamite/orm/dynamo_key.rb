module Dynamite
  class DynamoKey

    SEPARATOR = ':'

    attr_accessor :primary, :range
    def initialize(primary, range=nil)
      self.primary = primary
      self.range = range
    end

    def range?
      !self.range.nil?
    end

    def to_s
      self.range.nil?  ? self.primary : "#{self.primary}#{SEPARATOR}#{self.range}"
    end

    def self.from(string)
      return nil if string.blank?
      parts = string.split(SEPARATOR)
      case parts.size
      when 1
        self.new(parts[0])
      when 2
        self.new(parts[0...-1].join(SEPARATOR), parts[-1])
      else
        raise Exception.new("Malformed DynamoKey: #{string}.")
      end
    end

    def eql?(object)
      object.is_a?(::Dynamite::DynamoKey) && self.primary == object.primary && self.range == object.range
    end

    def ==(object)
      eql?(object)
    end

    def to_msgpack(arg)
      self.to_s.to_msgpack(arg)
    end
  end
end