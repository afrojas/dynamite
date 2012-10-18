module Dynamite
  class STS
    HOST = 'sts.amazonaws.com'

    attr_accessor :access_key, :secret_key, :request_time

    def initialize
      self.access_key = Dynamite.config.access_key
      self.secret_key = Dynamite.config.secret_key
    end

    def credentials
      if Dynamite.config.development?
        # In development we work against fake_dynamo, which doesn't require a trip out to Amazon to get proper creds
        # Also allows for Internet-free development!
        hash = {
          'token'       => 'random',
          'secret_key'  => 'ass',
          'access_key'  => 'shit',
          'expiration'  => DateTime.now.to_s
        }
        return hash
      end
      self.request_time = Time.now.utc

      headers = {
        "content-type" => 'application/x-www-form-urlencoded; charset=utf-8',
        'host' => HOST,
        "x-amz-date" => request_time_long
      }

      payload = 'Action=GetSessionToken&DurationSeconds=3600&Version=2011-06-15'
      canonical_request = create_canonical_request(headers, payload)
      string_to_sign = create_string_to_sign(canonical_request)
      signing_key = create_signing_key
      signature = hexy(hmac(signing_key, string_to_sign))

      headers.merge!(authorization_header(headers, signature))

      credentials = {}
      Dynamite.async do
        # Despite the Goliath example, we still need to wrap the http request in a synchrony block to achieve async behavior.
        http = EM::Synchrony.sync EventMachine::HttpRequest.new("https://#{HOST}").apost(:head => headers, :body => payload)

        response = Nokogiri::XML(http.response)
        credentials['token'] = response.css('SessionToken').text
        credentials['secret_key'] = response.css('SecretAccessKey').text
        credentials['access_key'] = response.css('AccessKeyId').text
        credentials['expiration'] = response.css('Expiration').text
      end
      return credentials
    end

    def request_time_long
      request_time.strftime("%Y%m%dT%H%M%SZ")
    end

    def request_time_short
      request_time.strftime("%Y%m%d")
    end

    def authorization_header(headers, signature)
      credential = "#{access_key}/#{request_time_short}/#{aws_region}/#{aws_service}/aws4_request"
      {'Authorization' => "AWS4-HMAC-SHA256 Credential=#{credential}, SignedHeaders=#{signed_headers}, Signature=#{signature}"}
    end

    def create_signing_key
      date_key = hmac("AWS4#{@secret_key}", request_time_short)
      region_key = hmac(date_key, aws_region)
      service_key = hmac(region_key, aws_service)
      signing_key = hmac(service_key, 'aws4_request')
    end

    def hexy(binary)
      result = ""
      data = binary.unpack("C*")
      data.each {|b| result += "%02x" % b}
      result
    end

    def hmac(key, string)
      OpenSSL::HMAC.digest('sha256', key, string)
    end

    def create_string_to_sign(canonical_request)
      ['AWS4-HMAC-SHA256', request_time_long, credential_scope, hex_encode(hash(canonical_request))].join("\n")
    end

    def credential_scope
      [request_time_short, aws_region, aws_service, 'aws4_request'].join("/")
    end

    def aws_region
      # can't use us-west-1
      'us-east-1'
    end

    def aws_service
      'sts'
    end

    def create_canonical_request(headers, payload)
      parts = []
      parts << http_request_method
      parts << canonical_uri
      parts << canonical_query_string
      parts << canonical_headers(headers)
      parts << signed_headers
      parts << hex_encode(hash(payload))

      parts.join("\n")
    end

    def http_request_method
      'POST'
    end

    def canonical_uri
      '/'
    end

    def canonical_query_string
      ''
    end

    def canonical_headers(headers)
      # Normally would need to ensure all header keys are downcased,
      # keys and values have whitespaced stripped, all keys are sorted,
      # and authorization header is not included.  But we don't have any spurious headers,
      # so we can skip those steps.
      headers.map{|k,v| "#{k}:#{v}\n"}.join('')
    end

    # Basically list of all the headers we just iterated through for the canonical headers
    # Since we know our exact headers, don't need to actually calculate this
    def signed_headers
      'content-type;host;x-amz-date'
    end

    def hex_encode(string)
      string.unpack('H*').first
    end

    def hash(string)
      Digest::SHA256.digest(string)
    end

  end
end