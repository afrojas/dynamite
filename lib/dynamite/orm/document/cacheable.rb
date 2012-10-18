module Dynamite
  module Document
    module Cacheable

      def self.included(base)
        base.extend ::Dynamite::Document::Cacheable::ClassMethods
      end

      def invalidate_cache
        if ::Dynamite.config.redis
          ::Dynamite.config.redis.del(self.class.cache_key(self.dynamo_key))
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

        def invalidate_cache
          if ::Dynamite.config.production?
            ::Dynamite.log.warn("You can't run this on production, it might be too slow.")
          else
            if ::Dynamite.config.redis
              keys = ::Dynamite.config.redis.keys("#{self.to_s}:*")
              ::Dynamite.config.redis.del(keys) unless keys.blank?
            end
          end
        end
      end # End ClassMethods

    end
  end
end