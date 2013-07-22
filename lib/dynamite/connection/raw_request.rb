module Dynamite
  class DynamoDB
    module RawRequest

      attr_accessor :access_key, :secret_key, :token, :expiration, :host, :port

      def renew_credentials
        credentials = ::Dynamite::STS.new.credentials
        self.access_key = credentials['access_key']
        self.secret_key = credentials['secret_key']
        self.token = credentials['token']
        self.expiration = DateTime.parse(credentials['expiration']).to_time
        self.host = ::Dynamite.config.endpoint
        self.port = ::Dynamite.config.port
      end

      def request(command, payload)
        # if we are within a minute of expiring, renew credentials
        if Time.now  > self.expiration - 60
          renew_credentials
        end

        headers = compose_headers(command)
        headers_to_sign = filter_headers(headers)
        string_to_sign = create_string_to_sign(headers, headers_to_sign, payload)
        signature = sign(string_to_sign)

        # add the signature to the headers
        headers["x-amzn-authorization"] =
          "AWS3 AWSAccessKeyId=#{access_key},Algorithm=HmacSHA256,SignedHeaders=#{headers_to_sign.join(';')},Signature=#{signature}"

        Dynamite.log.info("DYNAMITE:: Request To Dynamo: #{command} #{payload}") if Dynamite.config.verbose_logging
        ::Dynamite.async do
          # Despite the Goliath example, we still need to wrap the http request in a synchrony block to achieve async behavior.
          http = EM::Synchrony.sync EventMachine::HttpRequest.new("#{host}:#{port}").apost(:head => headers, :body => payload)
          http.response
        end
      end

      private
      def host_domain
        host =~ /https?:\/\/(.*)/
        $1
      end

      # order headers alphabetically to save on sorting later
      def compose_headers(command)

        headers = {
          'content-type' => 'application/x-amz-json-1.0',
          'host' => host_domain,
          'x-amz-date' => Time.now.rfc822,
          'x-amz-security-token' => token,
          'x-amz-target' => "DynamoDB_20111205.#{command}"
        }
      end

      def create_string_to_sign(headers, keys, body)
        ['POST', "/", "", canonical_headers(headers, keys), body].join("\n")
      end

      def canonical_headers(headers, keys)
        # Normally need to sort the headers before returning, but we pre-sorted when composing the headers hash
        # We also are supposed to ensure that header keys are downcased, and both headers and values have
        # whitespace stripped - but again, we've already ensured that.
        keys.map do |name|
          value = headers[name]
          "#{name}:#{value}\n"
        end.join
      end

      def filter_headers(headers)
        headers.keys.select do |header|
          header == "host" ||
          header == "content-encoding" ||
          header =~ /^x-amz/
        end
      end

      def sign(string)
        hash = OpenSSL::Digest::SHA256.digest(string)
        digest = OpenSSL::Digest::Digest.new('sha256')
        Base64.encode64(OpenSSL::HMAC.digest(digest, secret_key, hash)).strip
      end

    end
  end
end