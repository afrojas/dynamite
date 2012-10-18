module Dynamite
  module Document
    module Polymorphic

      def polymorphic_type
        self.class.to_s
      end

      def polymorphic_type=(value)
        # no op
      end

      def self.included(base)
        base.mark_field_as_persistent(:polymorphic_type, :string)
        base.extend ::Dynamite::Document::Polymorphic::ClassMethods
      end

      module ClassMethods
        def polymorphic?
          true
        end
      end

    end
  end
end