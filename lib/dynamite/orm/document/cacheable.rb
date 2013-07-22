module Dynamite
  module Document
    module Cacheable

      def self.included(base)
        base.extend ::Dynamite::Document::Cacheable::ClassMethods
      end

      def invalidate_cache
        if ::Dynamite.config.redis
          ::Dynamite.config.redis.hdel(self.class.instances_cache_key, self.class.cache_key(self.dynamo_key))
          ::Dynamite.config.redis.del(self.class.class_cache_key)
        end
      end

      module ClassMethods
        def cacheable
          true
        end

        def cache_key(dynamo_key)
          # Key will look something like "EquipmentDescription:Hat:20"
          "#{self.to_s}:#{dynamo_key.to_s}"
        end

        def class_cache_key
          "#{self.to_s}:all"
        end

        def instances_cache_key
          "#{self.to_s}:instances"
        end

        def invalidate_cache
          if ::Dynamite.config.redis
            ::Dynamite.config.redis.del(instances_cache_key)
            ::Dynamite.config.redis.del(class_cache_key)
          end
        end
      end # End ClassMethods

    end
  end
end
