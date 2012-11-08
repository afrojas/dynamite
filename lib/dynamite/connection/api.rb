module Dynamite
  class DynamoDB
    module API
      include ::Dynamite::DynamoDB::RawRequest
      MAX_RETRIES = 3

      def api_request(type, options)
        dynamo_response = request(type, Oj.dump(options[:params]))
        begin
          response = Oj.load(dynamo_response)
        rescue Oj::ParseError => error
          if dynamo_response.blank?
            # Weird, try again 1 time.  For convenience, just do request and parsing together.
            response = Oj.load(request(type, Oj.dump(options[:params])))
          else
            raise BadJSONException.new("Bad JSON. Response: #{dynamo_response}.")
          end
        end

        begin
          detect_errors(response, type, options)
        rescue DynamoException => exception
          klass = options[:klass]
          if klass.retry_after_error?(exception, type, options)
            options[:iteration] = options[:iteration].to_i + 1
            return api_request(type, options)
          else
            raise exception
          end
        end

        Dynamite::DynamoDB::Stats.record_type(type, options)
        # if Dynamite::DynamoDB.write_delay > 0
        #   EM::Synchrony.sleep(Amazon::DynamoDB.write_delay)
        # end
        response
      end

      # Deprecated, not being used.
      # def list_tables(limit=10)
      #   params = {
      #     'Limit' => limit
      #   }
      #   api_request('ListTables', {:params => params})
      # end

      def describe_table(klass)
        options = {'TableName' => klass.table_name}
        # Go outside the standard flow, since we use this method to detect for the existance
        # of tables.  That means errors such as ResourceNotFound are part of the standard operating flow.
        Oj.load(request('DescribeTable', Oj.dump(options)))
      end

      def create_table(klass)
        params = {
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
          params['KeySchema']['RangeKeyElement'] = {
            'AttributeName' => klass.range_options[:name],
            'AttributeType' => klass.range_options[:type]
          }
        end
        api_request('CreateTable', {:params => params, :klass => klass})
      end

      def delete_table(klass)
        params = {'TableName' => klass.table_name}
        api_request('DeleteTable', {:params => params, :klass => klass})
      end

      def put_item(klass, item)
        fields = {'id' => {'S' => item.id}}
        item.class.persistent_fields.keys.each do |field|
          encoded = item.encode_field(field)
          fields[field.to_s] = encoded unless encoded.nil?
        end
        params = {
          'TableName' => klass.table_name,
          'Item' => fields
        }
        # Version number will be 0 for newly initialized objects, but incremented by #save before
        # this method is called.
        if item.version_number > 1
          params['Expected'] = {'version_number' => {'Value' => {'N' => (item.version_number - 1).to_s}}}
        elsif item.version_number == 1
          # Ensure that objects that are new don't overwrite other objects.
          params['Expected'] = {'id' => {'Exists' => false}}
        else
          raise Exception.new("What is wrong with the version number?  #{item.version_number}.")
        end
        api_request('PutItem', {:params => params, :klass => klass})
      end

      def get_item(klass, id, range=nil, consistent=false)
        params = {
          'TableName' => klass.table_name,
          'Key' => {
            'HashKeyElement' => {'S' => id}
          }
        }
        if range
          params['Key']['RangeKeyElement'] = {klass.range_options[:type] => range.to_s}
        end
        if consistent
          params['ConsistentRead'] = true
        end
        response = api_request('GetItem', {:params => params, :klass => klass})
        response['Item']
      end

      # TODO - almost identical to #get_item
      def delete_item(klass, id, range=nil)
        params = {
          'TableName' => klass.table_name,
          'Key' => {
            'HashKeyElement' => {'S' => id}
          }
        }
        if range
          params['Key']['RangeKeyElement'] = {klass.range_options[:type] => range.to_s}
        end
        api_request('DeleteItem', {params: params, klass: klass})
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
        params = {
          'RequestItems' => {
            klass.table_name => {
              'Keys' => keys
            }
          }
        }
        response = api_request('BatchGetItem', {params: params, klass: klass, iteration: iteration})
        items = response['Responses'][klass.table_name]['Items']

        # Sometimes Dynamo fails to find all the objects, try again.
        # TODO - go over this method again.  amazon probably returns all the keys formatted for us, do we really need to reformat them? also,
        # make sure the method still works now with params inside an options hash
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

      def scan(klass, options)
        params = {
          'TableName' => klass.table_name,
        }
        limit = options.delete('limit')
        params['Limit'] = limit if limit
        unless options.empty?
          scan_filter = {}
          options.each do |key, value|
            attribute_list = {'S' => value}
            scan_filter[key] = {
              'AttributeValueList' => [{klass.data_type_code(key) => value.to_s}],
              'ComparisonOperator' => 'EQ'
            }
          end
          params['ScanFilter'] = scan_filter
        end

        response = api_request('Scan', {params: params, klass: klass})
        response['Items']
      end

      def paginate(klass, options = {})
        params = {
          'TableName' => klass.table_name
        }

        limit = options.delete('limit')
        params['Limit'] = limit if limit

        if next_key = options.delete('next_key')
          # TODO do we need RangeKeyElement here?
          params['ExclusiveStartKey'] = {
            'HashKeyElement' => { 'S' => next_key }
          }
        end

        response = api_request('Scan', {params: params, klass: klass})
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
            expected = options[:params]['Expected']
            log_message = "Options: #{options}"
            if expected.values.first['Exists'] == false
              raise ItemAlreadyExistsException.new(log_message)
            elsif expected['version_number']
              raise DirtyWriteException.new(log_message)
            else
              raise BadExpectationsException.new(log_message)
            end
          elsif error_message =~ /configured provisioned throughput/
            operation = (type == 'PutItem') ? 'write' : 'read'
            raise ThroughputException.new("Throughput error: #{options[:klass]} needs more #{operation} throughput.")
          else
            raise DynamoException.new("#{error_message}\nOptions: #{options}.")
          end
        end
      end

    end
  end
end