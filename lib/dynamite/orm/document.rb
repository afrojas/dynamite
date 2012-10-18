module Dynamite
  module Document

    def self.included(base)
      base.extend ::Dynamite::Document::Callbacks
      base.extend ::Dynamite::Document::ClassMethods
      base.extend ::Dynamite::Document::Associations::ClassMethods
      base.send(:include, ::Dynamite::Document::InstanceMethods)
      base.send(:include, ::Dynamite::Document::Associations::InstanceMethods)
      base.field :version_number, :number
      base.field :created_at, :time
    end

  end
end