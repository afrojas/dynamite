module Dynamite
  class DynamoDB
    class Stats

      WRITE_TYPES = %w[CreateTable DeleteItem DeleteTable PutItem UpdateItem UpdateTable BatchWriteItem]
      FORGETTABLE_TYPES = %w[ListTables]

      def self.record_type(type, response, options = {})
        return unless ::Dynamite.config.record_stats
        return if FORGETTABLE_TYPES.include?(type)
        table_name = options[:params]['TableName'].to_s

        # If the response was an error, this will result in 0.
        increment = response['ConsumedCapacityUnits'].to_f

	      # BatchGet follows different structure of the JSON
        if type == 'BatchGetItem'
          table_name = options[:params]['RequestItems'].keys.first
          if response['Responses']
            increment = response['Responses'][table_name]['ConsumedCapacityUnits'].to_f
          end
        end

        # Special case Tput errors to have an increment value of -1
        if increment == 0 && response['message'] =~ /configured provisioned throughput/
          increment = -1
        end

        query_type = WRITE_TYPES.include?(type) ? :writes : :reads
        update_stats_for_table(table_name, query_type, increment)
      end

      def self.report
        str = '[%s r:%i w:%i]'
        return "" if message_stats.empty?
        # Log the stats
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

        Dynamite.config.log.info("STATS: #{str % [ "total", total_reads, total_writes ]}, #{result}")
      end

      def self.query_stats(table_name)
        message_stats[table_name] ||= { writes: 0, reads: 0 }
      end

      def self.message_stats
        Dynamite.config.session.get(:dynamo_stats) || {}
      end

      def self.update_stats_for_table(table_name, query_type, increment)
        emit_smoke_signal(table_name, query_type, increment)
        dynamo_stats = message_stats
        per_table = dynamo_stats[table_name] || {writes: 0, reads: 0}
        per_table[query_type] += increment
        dynamo_stats[table_name] = per_table
        Dynamite.config.session.set(:dynamo_stats, dynamo_stats)
      end

      def self.emit_smoke_signal(table_name, query_type, value)
        return if value == 0
        name = "tput:#{table_name}:#{query_type}"
        Dynamite.config.smoke_signals.track(name, value)
      end

    end
  end
end