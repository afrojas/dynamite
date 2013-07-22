class DynamoException < Exception
  attr_accessor :original_message, :details
  
  def initialize(message, details=nil)
    self.details = details
    self.original_message = message
    super(message)
  end
  
  def to_s
    msg = "#{original_message}"
    msg << "\n\nDetails:\n#{details}" unless details.nil?
    msg
  end
end

class BadExpectationsException < DynamoException
end

class DirtyWriteException < BadExpectationsException
end

class ItemAlreadyExistsException < BadExpectationsException
end

class TransactionNeededException < DynamoException
end

class ThroughputException < DynamoException
end

class BadJSONException < DynamoException
end

class TooManyRetriesException < DynamoException
end