module Dynamite
  module Document

    DEFAULT_WRITE_THROUGHPUT = 5
    DEFAULT_READ_THROUGHPUT = 10
    
    def self.included(base)
      base.extend ::Dynamite::Document::Callbacks
      base.extend ::Dynamite::Document::ClassMethods
      base.extend ::Dynamite::Document::Associations::ClassMethods
      base.send(:include, ::Dynamite::Document::InstanceMethods)
      base.send(:include, ::Dynamite::Document::Associations::InstanceMethods)
      base.field :version_number, :number
      base.field :server_version
      base.field :created_at, :time
    end

  end
end