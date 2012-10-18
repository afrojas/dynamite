class DynamoException < Exception
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