# -*- encoding: utf-8 -*-
require File.expand_path('../lib/dynamite/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Andrés Rojas"]
  gem.email         = ["afrojas@gmail.com"]
  gem.description   = %q{ORM backing to Amazon's DynamoDB}
  gem.summary       = %q{ORM backing to Amazon's DynamoDB}
  gem.homepage      = ""

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.name          = "dynamite"
  gem.require_paths = ["lib"]
  gem.version       = Dynamite::VERSION
  gem.add_dependency('hashie', '~> 1.2.0')
  gem.add_dependency('eventmachine', '~> 1.0.0')
  gem.add_dependency('em-synchrony', '~> 1.0.1')
  gem.add_dependency('em-http-request', '~> 1.0.3')
  gem.add_dependency('nokogiri', '~> 1.5.2')
  gem.add_dependency('oj', '~> 1.4.2')
  gem.add_dependency('activesupport', '~> 3.2.3')
  gem.add_dependency('msgpack', '~> 0.4.6')
end