module Dynamite
  class DynaPack
    def self.pack(obj)
      ::MessagePack.pack(obj).force_encoding('UTF-8')
    end

    def self.unpack(string)
      ::MessagePack.unpack(string.force_encoding('ASCII-8BIT'))
    end
  end
end