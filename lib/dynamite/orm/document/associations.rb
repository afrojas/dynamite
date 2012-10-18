module Dynamite
  module Document
    module Associations

      module InstanceMethods
        # Whenever an association is loaded from dynamo, mark it as dirty. Prior to saving, the current state of the association
        # will be copied into the corresponding _id(s) field in order to make sure the latest state is saved.
        # See Persistence::Dynamo#save
        def mark_field_as_dirty(field)
          dirty_associations << field.to_s
        end

        def dirty?(field)
          dirty_associations.include?(field.to_s)
        end

        def dirty_associations
          @dirty_associations ||= Set.new
        end
      end # End InstanceMethods

      # Associations
      # For now, not doing any inverse connections.
      # Example:  if a Dog has_many Fleas, and you do dog.fleas << flea,
      #           you still need to set flea.dog = dog.
      # We can change this if we want, but don't want to automatically be adding data
      # if we don't want/need to.
      module ClassMethods
        def mark_association(objects_field, ids_field)
          associations[objects_field] = ids_field
        end

        def associations
          @associations ||= {}
        end

        def belongs_to(symbol, options={})
          has_one(symbol, options)
        end

        def has_one(symbol, options={})
          field_name = "#{symbol}_id"
          mark_field_as_persistent(field_name, :dynamo_key)
          attr_accessor field_name
          mark_association(symbol, "#{field_name}=")

          klass = (options[:class_name] || symbol.to_s.camelize)
          variable_name = "@#{symbol}"

          define_method symbol do
            if instance_variable_defined?(variable_name)
              instance_variable_get(variable_name)
            else
              mark_field_as_dirty(symbol)
              dynamo_key = self.send(field_name)
              value = dynamo_key.blank? ? nil : klass.constantize.find(dynamo_key)
              instance_variable_set(variable_name, value)
            end
          end

          define_method "#{symbol}=" do |value|
            mark_field_as_dirty(symbol)
            self.send("#{field_name}=", value.nil? ? nil : value.dynamo_key)
            instance_variable_set(variable_name, value)
          end
        end

        def has_many(symbol, options={})
          singular = symbol.to_s.singularize
          field_name = "#{singular}_ids"
          mark_field_as_persistent(field_name, :dynamo_keys)
          attr_accessor field_name
          mark_association(symbol, "#{field_name}=")

          klass = (options[:class_name] || singular.camelize)
          variable_name = "@#{symbol}"

          define_method symbol do
            if instance_variable_defined?(variable_name)
              instance_variable_get(variable_name)
            else
              mark_field_as_dirty(symbol)
              ids = self.send(field_name)
              # Must dynamically constantize klass, otherwise it won't be found (not loaded up yet).
              values = ids.blank? ? [] : klass.constantize.find_all(ids)

              if options[:ordered] && !values.blank?
                values = ids.map do |id|
                  index = values.index{|value| value.id == id.to_s}
                  index.nil? ? index : values.delete_at(index)
                end
                values.compact!
              end
              instance_variable_set(variable_name, values)
            end
          end

          # TODO - handle setting to nil
          # TODO - also, can we handle adding to an association via array calls like #<< ?
          define_method "#{symbol}=" do |values|
            mark_field_as_dirty(symbol)
            values = [values] unless values.is_a?(Array)
            ids = values.map{|val| val.dynamo_key}
            self.send("#{field_name}=", ids)
            instance_variable_set(variable_name, values)
          end
        end
      end # End ClassMethods

    end
  end
end