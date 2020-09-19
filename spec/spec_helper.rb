# frozen_string_literal: true

require 'bundler/setup'
require 'pgq'
require 'pgq/active_record_adapter'

Pgq::API.adapter = Pgq::ActiveRecordAdapter.new(ActiveRecord::Base)

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = '.rspec_status'

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:suite) do
    database_config_path = File.join(__dir__, 'database.yml')
    database_config = YAML.load_file(database_config_path)
    ActiveRecord::Base.establish_connection(database_config)

    log_path = File.join(__dir__, 'test.log')
    File.truncate(log_path, 0) if File.exist?(log_path) && !ENV['KEEP_TEST_LOG']
    ActiveRecord::Base.logger = Logger.new(log_path)
    ActiveRecord::Base.logger.level = :debug

    Pgq::API.adapter.execute('DROP EXTENSION IF EXISTS pgq CASCADE;')
    Pgq::API.adapter.execute('CREATE EXTENSION pgq;')
  end

  config.after(:suite) do
    Pgq::API.adapter.execute('DROP EXTENSION IF EXISTS pgq CASCADE;')
  end
end
