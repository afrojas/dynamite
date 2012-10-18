module Dynamite
  class DynamoDB
    module API
      include ::Dynamite::DynamoDB::RawRequest
      MAX_RETRIES = 3

      def api_request(type, options, iteration=1)
        dynamo_response = request(type, Oj.dump(options))
        begin
          response = Oj.load(dynamo_response)
        rescue Oj::ParseError => error
          if dynamo_response.blank?
            # Weird, try again 1 time.  For convenience, just do request and parsing together.
            response = Oj.load(request(type, Oj.dump(options)))
          else
            raise BadJSONException.new("Bad JSON. Response: #{dynamo_response}.")
          end
        end

        begin
          detect_errors(response, type, options)
        rescue ThroughputException => ex
          if iteration > MAX_RETRIES
            raise ex
          else
            ex.message =~ /Throughput error: (.+)/
            # EmailLogger.log("Retrying due to throughput limits: #{$1}")
            # sleep for > 1 second to let throughput limit reset, and then try again.
            # Since we are also emailing, probably don't need to sleep a full second.
            # TODO: eventually may want to change to non-email logging, or at least adjusting sleep time.
            sleep 1
            return api_request(type, options, iteration + 1)
          end
        end
        Dynamite::DynamoDB::Stats.record_type(type, options)
        # if Dynamite::DynamoDB.write_delay > 0
        #   sleep Dynamite::DynamoDB.write_delay
        # end
        response
      end

      def list_tables(limit=10)
        options = {
          'Limit' => limit
        }
        api_request('ListTables', options)
      end

      def describe_table(klass)
        options = {'TableName' => klass.table_name}
        # Don't throw a blanket exception, because we use this method to check for the existence
        # of tables.  Can start using #api_request if we refactor #detect_errors
        Oj.load(request('DescribeTable', Oj.dump(options)))
      end

      def create_table(klass)
        options = {
          'TableName' => klass.table_name,
          'KeySchema' => {
            'HashKeyElement' => {
              'AttributeName' => 'id',
              'AttributeType' => 'S'
            }
          },
          'ProvisionedThroughput' => {
            'ReadCapacityUnits' => 5,
            'WriteCapacityUnits' => 10
          }
        }
        if klass.range_options
          options['KeySchema']['RangeKeyElement'] = {
            'AttributeName' => klass.range_options[:name],
            'AttributeType' => klass.range_options[:type]
          }
        end
        api_request('CreateTable', options)
      end

      def delete_table(klass)
        options = {'TableName' => klass.table_name}
        request('DeleteTable', Oj.dump(options))
      end

      def put_item(table_name, item)
        fields = {'id' => {'S' => item.id}}
        item.class.persistent_fields.keys.each do |field|
          encoded = item.encode_field(field)
          fields[field.to_s] = encoded unless encoded.nil?
        end
        options = {
          'TableName' => table_name,
          'Item' => fields
        }
        # Version number will be 0 for newly initialized objects, but incremented by #save before
        # this method is called.
        if item.version_number > 1
          options['Expected'] = {'version_number' => {'Value' => {'N' => (item.version_number - 1).to_s}}}
        elsif item.version_number == 1
          # Ensure that objects that are new don't overwrite other objects.
          options['Expected'] = {'id' => {'Exists' => false}}
        else
          raise Exception.new("What is wrong with the version number?  #{item.version_number}.")
        end
        api_request('PutItem', options)
      end

      def get_item(klass, id, range=nil, consistent=false)
        options = {
          'TableName' => klass.table_name,
          'Key' => {
            'HashKeyElement' => {'S' => id}
          }
        }
        if range
          options['Key']['RangeKeyElement'] = {klass.range_options[:type] => range.to_s}
        end
        if consistent
          options['ConsistentRead'] = true
        end
        response = api_request('GetItem', options)
        response['Item']
      end

      # TODO - almost identical to #get_item
      def delete_item(klass, id, range=nil)
        options = {
          'TableName' => klass.table_name,
          'Key' => {
            'HashKeyElement' => {'S' => id}
          }
        }
        if range
          options['Key']['RangeKeyElement'] = {klass.range_options[:type] => range.to_s}
        end
        api_request('DeleteItem', options)
      end

      def batch_get_item(klass, ids, iteration=1)
        if iteration > MAX_RETRIES
          raise TooManyRetriesException.new("Attempt #{iteration}, have #{ids.size} #{klass} objects left.")
        end
        keys = ids.map do |id|
          if id.is_a?(::Dynamite::DynamoKey)
            hash = {'HashKeyElement' => {'S' => id.primary}}
            if id.range?
              hash['RangeKeyElement'] = {klass.range_options[:type] => id.range.to_s}
            end
            hash
          else
            {'HashKeyElement' => {'S' => id}}
          end
        end
        options = {
          'RequestItems' => {
            klass.table_name => {
              'Keys' => keys
            }
          }
        }
        response = api_request('BatchGetItem', options)
        items = response['Responses'][klass.table_name]['Items']

        # Sometimes Dynamo fails to find all the objects, try again.
        unprocessed_keys = response['UnprocessedKeys']
        if unprocessed_keys.size > 0
          array = unprocessed_keys[klass.table_name]['Keys']
          unprocessed_ids = array.map do |element|
            hash_key = element['HashKeyElement']['S']
            range_key = element['RangeKeyElement'][klass.range_options[:type]] if element['RangeKeyElement']
            ::Dynamite::DynamoKey.new(hash_key, range_key)
          end
          ::Dynamite.log.info("BatchGetItem attempt ##{iteration}, have #{unprocessed_ids.size} #{klass} objects left out of #{ids.size} to get.")
          items.concat(batch_get_item(klass, unprocessed_ids, iteration + 1))
        end
        items
      end

      def scan(klass, params)
        options = {
          'TableName' => klass.table_name,
        }
        limit = params.delete('limit')
        options['Limit'] = limit if limit
        unless params.empty?
          scan_filter = {}
          params.each do |key, value|
            attribute_list = {'S' => value}
            scan_filter[key] = {
              'AttributeValueList' => [{klass.data_type_code(key) => value.to_s}],
              'ComparisonOperator' => 'EQ'
            }
          end
          options['ScanFilter'] = scan_filter
        end

        response = api_request('Scan', options)
        response['Items']
      end

      def paginate(klass, params = {})
        options = {
          'TableName' => klass.table_name
        }

        limit = params.delete('limit')
        options['Limit'] = limit if limit

        if next_key = params.delete('next_key')
          # TODO do we need RangeKeyElement here?
          options['ExclusiveStartKey'] = {
            'HashKeyElement' => { 'S' => next_key }
          }
        end

        response = api_request('Scan', options)
        result = { 'objects' => response['Items'] }

        if response['LastEvaluatedKey']
          result['next_key'] = response['LastEvaluatedKey']['HashKeyElement']['S']
        end

        result
      end

      def detect_errors(response, type, options)
        if response['__type'] =~ /Exception/
          error_message = response['message'] || response['Message']  # Lame Amazon is not consistent
          if error_message =~ /The conditional request failed/
            expected = options['Expected']
            log_message = "Options: #{options}"
            if expected.values.first['Exists'] == false
              raise ItemAlreadyExistsException.new(log_message)
            elsif expected['version_number']
              raise DirtyWriteException.new(log_message)
            else
              raise BadExpectationsException.new(log_message)
            end
          elsif error_message =~ /configured provisioned throughput/
            table = options['TableName']
            if table.nil?
              hash = options['RequestItems']
              table = hash.first[0] if hash
            end
            table =~ /#{Wizards.environment}_(\w+)/
            table = $1
            operation = (type == 'PutItem') ? 'write' : 'read'
            raise ThroughputException.new("Throughput error: #{table} needs more #{operation} throughput.")
          else
            raise DynamoException.new("#{error_message}\nOptions: #{options}.")
          end
        end
      end

    end
  end
end