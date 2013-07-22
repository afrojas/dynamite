module Dynamite
  module Document
    module InstanceMethods

      attr_accessor :id, :persisted

      def table_name
        # TODO - is there an issue prefixing all tables with the same prefix (in terms of hash balancing)?
        self.class.table_name
      end

      def initialize(options={})
        self.class.persistent_fields.keys.each do |key|
          self.send("#{key}=", self.class.persistent_field_default(key))
        end
        self.id = SecureRandom.uuid
        self.persisted = false
        self.version_number = 0
        self.server_version = '1.0.0'
        self.created_at = Time.now
        options.each do |key, value|
          self.send("#{key}=", value)
        end
        if self.respond_to?(:initialize_defaults) && !self.persisted
          self.initialize_defaults
        end

        self.class.execute_callbacks_for(self, ::Dynamite::Document::Callbacks::AFTER_INITIALIZE)
      end

      def to_s
        "#{self.class}: #{dynamo_key}"
      end

      def inspect
        fields = {}
        self.class.persistent_fields.keys.sort.each do |field|
          fields[field] = self.send(field)
        end
        "#{self.class} #{fields}"
      end

      def copy(persist=false)
        clone = self.clone
        clone.id = SecureRandom.uuid
        clone.persisted = false
        clone.version_number = 0
        clone.server_version = '1.0.0'
        clone.save if persist
        clone
      end

      def save
        if self.class.concurrency_enforced && !self.class.in_transaction
          # We need a transaction to protect against concurrent saves, unless we are creating a new object.
          raise TransactionNeededException.new("Saving a #{self.class}, which requires a transaction.") if persisted
        end
        is_first_time = false
        unless persisted
          # First time saving
          is_first_time = true
          self.class.execute_callbacks_for(self, ::Dynamite::Document::Callbacks::BEFORE_CREATE)
        end
        self.class.execute_callbacks_for(self, ::Dynamite::Document::Callbacks::BEFORE_SAVE)

        # refresh any dirty associations before saving, to ensure we have the latest ids
        self.dirty_associations.each do |field|
          objects = self.send(field)
          if objects.respond_to?(:map)
            # Dealing with a collection of objects
            ids = objects.map{|obj| obj.dynamo_key}
          elsif objects
            ids = objects.dynamo_key
          else
            ids = nil
          end
          ids_field = self.class.associations[field.to_sym]
          if ids_field.nil?
            # Only works one level deep.
            ids_field = self.class.superclass.associations[field.to_sym]
          end
          self.send(ids_field, ids)
        end
        self.persisted = true
        self.version_number += 1
        self.server_version = Dynamite.config.server_version
        self.class.connection.put_item(self.class, self)
        invalidate_cache if self.class.cacheable && ::Dynamite.config.redis
        # TODO - handle errors
        self.class.execute_callbacks_for(self, ::Dynamite::Document::Callbacks::AFTER_CREATE) if is_first_time
        self.class.execute_callbacks_for(self, ::Dynamite::Document::Callbacks::AFTER_SAVE)

        self
      end

      def delete
        invalidate_cache if self.class.cacheable && ::Dynamite.config.redis
        self.class.connection.delete_item(self.class, id, range_key)
      end

      def update_attributes(set)
        self.class.transaction(self) do |object|
          set.each do |key, value|
            object.send("#{key}=", value)
          end
        end
      end

      def update_attribute(key, value)
        update_attributes(key => value)
      end

      def range_key
        self.class.range_options ? self.send(self.class.range_options[:name]) : nil
      end

      # Note that this does NOT edit the underlying object.  Never understood why ActiveRecord
      # did not append a ! to #reload, since their version does edit the underlying object.
      # If this causes confusion, can change it to match AR version later.
      #
      # Note that this does a consistent read, meaning it will be slower and more expensive than
      # regular reads.
      def reload
        object = self.class.find(dynamo_key, true)
        if object.nil?
          # What the fuck, how can an existing object not be found on a consistent read?  
          # Deleted by someone else maybe?
          # Try one more time
          EM::Synchrony.sleep(1.1)
          Log.warn("Reloaded object was nil, trying to find #{self.class}:#{self.dynamo_key}.")
          object = self.class.find(dynamo_key, true)
        end
        object
      end

      def refresh(field)
        ivar = "@#{field}"
        remove_instance_variable(ivar) if instance_variable_defined?(ivar)
        self.send(field)
      end

      # Returns a Hash with the field's value properly encoded for DynamoDB, keyed by the appropriate data type code.
      # There is a corresponding class level method #decode_field.
      def encode_field(field)
        value = self.send(field)
        # DynamoDB doesn't accept nils.
        # TODO - what happens if we previously have a value, and want to nil it out?
        return nil if value.nil? || (value.respond_to?(:blank?) && value.blank?)

        data_type = self.class.persistent_field_type(field)

        # Default to String data type
        data_type_code = 'S'
        case data_type
        when :dynamo_key
          # Has_one, belongs_to values are actually stored as DynamoKey objects, so convert to String.
          value = value.to_s
        when :dynamo_keys
          # Has_many associations are stored as Arrays of DynamoKey objects.
          value = Base64.strict_encode64(::Dynamite::DynaPack.pack(value))
          data_type_code = 'B' if ::Dynamite.config.public_environment?
        when :serialized
          # Marshal.dump will produce binary values that DynamoDB can't handle.
          # Use strict encoding to avoid newlines which cause problems (at least with Fake Dynamo)
          value = Base64.strict_encode64(Marshal.dump(value))
          data_type_code = 'B' if ::Dynamite.config.public_environment?
        when :numbers
          # Must be a unique set of numbers.
          data_type_code = 'NS'
        when :strings
          # Must be a unique set of strings.
          data_type_code = 'SS'
        when :number, :time, :boolean
          data_type_code = 'N'
          case data_type
          when :time
            value = value.to_i
          when :boolean
            value = value ? '1' : '0'
          end
          # number values are still sent as strings
          # this means that times are first converted to numbers, then to strings
          value = value.to_s
        end
        {data_type_code => value}
      end

      def dynamo_key
        ::Dynamite::DynamoKey.new(id, range_key)
      end

      def eql?(object)
        object.respond_to?(:dynamo_key) && object.dynamo_key == self.dynamo_key
      end

      def ==(object)
        eql?(object)
      end

    end
  end
end
