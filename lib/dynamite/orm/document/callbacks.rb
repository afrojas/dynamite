module Dynamite
  module Document
    module Callbacks
      AFTER_FIND = :after_find
      AFTER_INITIALIZE = :after_initialize
      AFTER_SAVE = :after_save
      AFTER_CREATE = :after_create
      BEFORE_CREATE = :before_create
      BEFORE_SAVE = :before_save

      def add_callback(type, method)
        list = callbacks_for(type)
        list << method
        callbacks[type] = list
      end

      def callbacks
        @callbacks ||= {}
      end

      def callbacks_for(type)
        callbacks[type] || []
      end

      # TODO - should be an instance method, but currently only extend class object with these methods.
      def execute_callbacks_for(object, type)
        callbacks_for(type).each do |callback|
          object.send(callback)
        end
        self.superclass.callbacks_for(type).each do |callback|
          object.send(callback)
        end if self.superclass.respond_to?(:callbacks_for)
      end

      def after_find(method)
        add_callback(AFTER_FIND, method)
      end

      def after_initialize(method)
        add_callback(AFTER_INITIALIZE, method)
      end

      def after_save(method)
        add_callback(AFTER_SAVE, method)
      end

      def after_create(method)
        add_callback(AFTER_CREATE, method)
      end

      def before_create(method)
        add_callback(BEFORE_CREATE, method)
      end

      def before_save(method)
        add_callback(BEFORE_SAVE, method)
      end

    end
  end
end