module Dynamite
  module Document
    module ClassMethods

      attr_accessor :concurrency_enforced, :in_transaction, :cacheable

      def create(options={})
        object = self.new(options)
        object.save
      end

      def connection
        @connection ||= ::Dynamite::DynamoDB.instance
      end

      def table(name)
        @table = name
      end

      def table_name
        "#{::Dynamite.config.environment}_#{@table || self.to_s.underscore.pluralize}"
      end

      def exists?
        response = connection.describe_table(self)
        (response['__type'] =~ /ResourceNotFoundException/).nil?
      end

      def mark_field_as_persistent(name, type)
        persistent_fields[name.to_sym] = type
      end

      # TODO: eventually need to convert all field values to Hashes, and then can remove the Hash check
      def persistent_field_type(name)
        data = persistent_fields[name.to_sym]
        data.is_a?(Hash) ? data[:data_type] : data
      end

      def persistent_field_default(name)
        data = persistent_fields[name.to_sym]

        # Make sure we duplicate the default, in case it's a collection object.
        # Otherwise, we will modify the collection object in place
        if data.is_a?(Hash)
          default = data[:default]
          default.duplicable? ? default.dup : default
        else
          nil
        end
      end

      def persistent_fields
        if @persistent_fields.nil?
          @persistent_fields = superclass.respond_to?(:persistent_fields) ? superclass.persistent_fields.dup : {}
        end
        @persistent_fields
      end

      def decode_field(field, value, data_type_code)
        value = value.to_i if data_type_code == 'N'
        case persistent_field_type(field)
        when :time
          value = Time.at(value)
        when :boolean
          value = (value.to_i == 1)
        when :dynamo_key
          value = ::Dynamite::DynamoKey.from(value)
        when :dynamo_keys
          strings = ::Dynamite::DynaPack.unpack(Base64.strict_decode64(value))
          value = strings.map{|string| ::Dynamite::DynamoKey.from(string)}
        when :serialized
          # DynamoDB can't handle binary data, so all values are strictly encoded before saving.
          # See DynamoDB::API#put_item
          value = Marshal.load(Base64.strict_decode64(value))
        end
        value
      end

      def find(id, consistent=false)
        return nil if id.blank?
        id = ::Dynamite::DynamoKey.from(id) unless id.is_a?(::Dynamite::DynamoKey)
        if cacheable && !consistent && ::Dynamite.config.redis
          serialized = ::Dynamite.config.redis.get(cache_key(id))
          return Marshal.load(serialized) if serialized
        end
        object = from_dynamo(connection.get_item(self, id.primary, id.range, consistent))
        if cacheable && ::Dynamite.config.redis
          ::Dynamite.config.redis.set(cache_key(id), Marshal.dump(object))
        end
        object
      end

      def raw_find(id, consistent=false)
        id = ::Dynamite::DynamoKey.from(id) unless id.is_a?(::Dynamite::DynamoKey)
        connection.get_item(self, id.primary, id.range, consistent)
      end

      def find_all(ids)
        return [] if ids.blank?
        if ids.size > 100
          # TODO: do we ever need to support this?
          raise DynamoException.new("Cannot get more than 100 objects from a batch get.")
        end

        connection.batch_get_item(self, ids).map{|hash| from_dynamo(hash)}
      end

      # Checks to make sure the id is contained within the collection, and then will go load the object.
      # Mimics the behavior of #find on a Rails association
      def locate(collection, *id)
        key = id[0].is_a?(::Dynamite::DynamoKey) ? key = id[0] : ::Dynamite::DynamoKey.new(*id)
        (collection && collection.include?(key)) ? find(key) : nil
      end

      def locate_all(collection, ids)
        ids.map{|id| locate(collection, id)}.compact
      end

      def all(opts = {})
        if cacheable && ::Dynamite.config.redis
          serialized = ::Dynamite.config.redis.get(class_cache_key)
          return Marshal.load(serialized) if serialized
        end

        objects = connection.scan(self, opts)
        objects.map! { |obj| from_dynamo(obj) }
        if cacheable && ::Dynamite.config.redis
          ::Dynamite.config.redis.set(class_cache_key, Marshal.dump(objects))
        end
        objects
      end

      def paginate(opts = {})
        result = connection.paginate(self, opts)
        result['objects'].map! { |obj| from_dynamo(obj) }
        result['next_key'] = result['next_key']
        result
      end

      def first
        all.first
      end

      def last
        all.last
      end

      def where(params)
        objects = connection.scan(self, params)
        objects.map{|obj| from_dynamo(obj)}
      end

      def blank_object(options)
        klass = options.delete(:klass)
        obj = klass ? klass.new(options) : self.new(options)
        obj
      end

      def from_dynamo(hash)
        return nil if hash.nil?
        begin
          # Start with blank object (all defaults for persisted fields), since Dynamo
          # does not allow for the storage of empty strings or nulls.

          if self.respond_to?(:polymorphic?) && self.polymorphic?
            # Fall back to #type for now
            klass = hash['polymorphic_type'] ? hash['polymorphic_type']['S'] : hash['type']['S']
            klass = klass.constantize
          end
          obj = blank_object(:id => hash['id']['S'], :persisted => true, :klass => klass)

          # Now set (override) all values retrieved from Dynamo
          hash.each do |field, value_hash|
            data_type_code = value_hash.keys.first
            value = decode_field(field, value_hash[data_type_code], data_type_code)
            obj.send("#{field}=", value)
          end

          obj.class.execute_callbacks_for(obj, ::Dynamite::Document::Callbacks::AFTER_FIND)
          obj
        rescue Exception => ex
          # For now, log every exception
          puts "Error loading from dynamo: #{ex.message}"
          puts ex.backtrace.join("\n")
          ::Dynamite.log.error("Error loading from dynamo: #{ex.message}")
          ::Dynamite.log.error(ex.backtrace.join("\n"))
          nil
        end
      end

      # Supported data_types:
      #   :string
      #   :number (floating points untested)
      #   :serialized
      #   :boolean
      #   :numbers (unique set of numbers)
      #   :strings (unique set of strings)
      #   :time
      def field(symbol, options=:string)
        mark_field_as_persistent(symbol, options)
        attr_accessor symbol
      end

      # S, N, SS, or NS
      def data_type_code(attribute)
        code = 'S'
        type = persistent_field_type(attribute)
        case type
        when :number, :time, :boolean
          code = 'N'
        when :strings
          code = 'SS'
        when :numbers
          code = 'NS'
        when :dynamo_keys, :serialized
          code = ::Dynamite.config.production? ? 'B' : 'S'
        end
        code
      end

      def primary_key(key)
        define_method "#{key}=" do |value|
          self.id = value
        end

        define_method key do
          self.id
        end
      end

      def range_options
        @range_options
      end

      def range_key(symbol, type=:string)
        type_code = case type
        when :number, :time
          'N'
        when :string
          'S'
        else
          raise Exception.new('Unsupported range key type.')
        end
        @range_options = {:name => symbol.to_s, :type => type_code}
        field(symbol, type)
      end

      # Execute the statements in the block.  If the block returns a non-false value,
      # attempt to save the object.  If the save fails due to a dirty flag, start over from
      # the beginning.
      def transaction(object, &block)
        retries = 3
        while retries > 0
          result = yield object
          break unless result
          begin
            self.in_transaction = true
            object.save
            self.in_transaction = false
            retries = 0
          rescue BadExpectationsException => ex
            ::Dynamite.log.info "#{object.class}:#{object.id} dirty, trying transaction again."
            retries -= 1
            object = object.reload
          end
        end
        # Return the object at the end, regardless of what happens
        object
      end

      def enforce_concurrency
        self.concurrency_enforced = true
      end

      def delete(id, range_key=nil)
        id = ::Dynamite::DynamoKey.from(id) unless id.is_a?(::Dynamite::DynamoKey)
        self.connection.delete_item(self, id.primary, id.range)
      end

    end
  end
end