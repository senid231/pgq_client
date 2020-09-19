# frozen_string_literal: true

require 'active_record'
require 'pg'
require 'pgq'

module Pgq
  # An abstract class of SQL adapter.
  # create a subclass and define methods #select_all #select_value #select_values #execute.
  class ActiveRecordAdapter < AbstractAdapter
    delegate :connection, to: :model_class

    def initialize(model_class)
      @model_class_name = model_class.to_s
      @model_class = model_class if model_class.is_a?(ActiveRecord::Base)
    end

    def execute(sql, *bindings)
      sql_query = sanitize_sql_array(sql, *bindings)
      connection.execute(sql_query)
    end

    def select_all(sql, *bindings)
      sql_query = sanitize_sql_array(sql, *bindings)
      result = connection.select_all(sql_query)
      result.map do |row|
        row.map { |k, v| [k.to_sym, result.column_types[k].deserialize(v)] }.to_h
      end
    end

    def select_value(sql, *bindings)
      sql_query = sanitize_sql_array(sql, *bindings)
      connection.select_value(sql_query)
    end

    def select_values(sql, *bindings)
      sql_query = sanitize_sql_array(sql, *bindings)
      connection.select_values(sql_query)
    end

    def model_class
      return @model_class if defined?(@model_class)

      @model_class = @model_class_name.constantize
    end

    def sanitize_sql_array(sql, *bindings)
      model_class.send(:sanitize_sql_array, [sql, *bindings])
    end
  end
end
