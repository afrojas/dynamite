module Dynamite
  class DynamoDB
    class Stats

      WRITE_TYPES = %w[CreateTable DeleteItem DeleteTable PutItem UpdateItem UpdateTable BatchWriteItem]
      FORGETTABLE_TYPES = %w[ListTables]

      @@queries = {}

      def self.record_type(type, options = {})
        return if FORGETTABLE_TYPES.include?(type)
        table_name = options[:klass].to_s

        if WRITE_TYPES.include?(type)
          query_stats(table_name)[:writes] += 1
        else
          query_stats(table_name)[:reads] += 1
        end
      end

      def self.clear
        @@queries.delete(message_id)
      end

      def self.report(str)
        return "" if message_stats.empty?
        total_reads = 0
        total_writes = 0

        result = [].tap do |result|
          sorted_keys = message_stats.keys.sort
          sorted_keys.each do |table_name|
            total_reads += reads = message_stats[table_name][:reads]
            total_writes += writes = message_stats[table_name][:writes]
            result << str % [ table_name, reads, writes ]
          end
        end.join(", ")

        "#{str % [ "total", total_reads, total_writes ]}, #{result}"
      end

      def self.query_stats(table_name)
        message_stats[table_name] ||= { writes: 0, reads: 0 }
      end

      def self.message_stats
        @@queries[message_id] ||= {}
      end

      def self.message_id
        Fiber.current.instance_variable_get(:@message_id) || "unknown"
      end

    end
  end
end