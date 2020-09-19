# frozen_string_literal: true

module Pgq
  # An abstract class of SQL adapter.
  # create a subclass and define methods #select_all #select_one #select_value.
  class AbstractAdapter
    # Executes sql statement or command.
    # @param sql [String]
    # @param bindings [Array]
    def execute(sql, *bindings)
      raise NotImplementedError
    end

    # Executes sql statement and return result rows.
    # @param sql [String]
    # @param bindings [Array]
    # @return [Array<Hash>] array with 0 or more hashes with symbol keys.
    #   hashes values types according to value type returned by postgresql.
    def select_all(sql, *bindings)
      raise NotImplementedError
    end

    # Executes sql statement and return result row.
    # @param sql [String]
    # @param bindings [Array]
    # @return [Hash,nil] hash with symbol keys.
    #   values types according to value type returned by postgresql.
    def select_one(sql, *bindings)
      select_all(sql, *bindings).first
    end

    # Executes sql statement and return result value.
    # @param sql [String]
    # @param bindings [Array]
    # @return [Object] value according to value type returned by postgresql.
    def select_value(sql, *bindings)
      raise NotImplementedError
    end

    # Executes sql statement and return result values.
    # @param sql [String]
    # @param bindings [Array]
    # @return [Array<Object>] array of values according to value type returned by postgresql.
    def select_values(sql, *bindings)
      raise NotImplementedError
    end
  end
end
