# frozen_string_literal: true

require 'singleton'
require 'forwardable'

module Pgq
  # Wrapping PgQ Skytools SQL functions
  # see http://pgq.github.io/extension/pgq/files/external-sql.html
  # see https://github.com/pgq/pgq/tree/master/functions
  class API
    include Singleton

    # @!method adapter= [Pgq::AbstractAdapter] instance of class that implements interface of Pgq::AbstractAdapter.
    # @!method adapter [Pgq::AbstractAdapter,nil] instance of class that implements interface of Pgq::AbstractAdapter.
    attr_accessor :adapter

    class << self
      extend Forwardable

      def_delegators :instance,
                     :adapter,
                     :adapter=,
                     :create_queue,
                     :drop_queue,
                     :drop_queue,
                     :set_queue_config,
                     :insert_event,
                     :insert_event,
                     :current_event_table,
                     :register_consumer,
                     :register_consumer_at,
                     :unregister_consumer,
                     :next_batch_info,
                     :next_batch,
                     :next_batch_custom,
                     :get_batch_events,
                     :get_batch_cursor,
                     :get_batch_cursor,
                     :event_retry,
                     :event_retry,
                     :batch_retry,
                     :finish_batch,
                     :get_queue_info,
                     :get_queue_info,
                     :get_consumer_info,
                     :get_consumer_info,
                     :get_consumer_info,
                     :version,
                     :get_batch_info,
                     :force_tick,
                     :ticker
    end

    # pgq.create_queue(1)	Creates new queue with given name.
    # @param queue_name [String]
    # @return [Boolean] false when queue already exist.
    def create_queue(queue_name)
      adapter.select_value('SELECT pgq.create_queue(?)', queue_name) == 1
    end

    # pgq.drop_queue(2)	Drop queue and all associated tables.
    # pgq.drop_queue(1)	Drop queue and all associated tables.
    # @param queue_name [String] queue name.
    # @param force [Boolean] ignore (drop) existing consumers when true.
    def drop_queue(queue_name, force: false)
      adapter.select_value('SELECT pgq.drop_queue(?, ?)', queue_name, force)
      nil
    end

    # pgq.set_queue_config(3)	Set configuration for specified queue.
    # @param queue_name [String] Name of the queue to configure.
    # @param param_name [String] Configuration parameter name.
    # @param param_value [String] Configuration parameter value.
    def set_queue_config(queue_name, param_name, param_value)
      adapter.select_value('SELECT pgq.set_queue_config(?, ?, ?)', queue_name, param_name, param_value)
      nil
    end

    # pgq.insert_event(3)	Insert a event into queue.
    # pgq.insert_event(7)	Insert a event into queue with all the extra fields.
    # @param queue_name [String] Name of the queue.
    # @param ev_type [String] User-specified type for the event.
    # @param ev_data [String] User data for the event.
    # @param extra [Array<String>,nil] nil or array of 4 or less text.
    # @return [Integer] event ID.
    def insert_event(queue_name, ev_type, ev_data, extra: nil)
      raise ArgumentError, 'extra array should have size 4 or less' if extra && extra.size > 4

      if extra.nil?
        adapter.select_value('SELECT pgq.insert_event(?, ?, ?)', queue_name, ev_type, ev_data)
      else
        extra += Array.new(4 - extra.size, nil)
        adapter.select_value('SELECT pgq.insert_event(?, ?, ?, ?, ?, ?, ?)', queue_name, ev_type, ev_data, *extra)
      end
    end

    # pgq.current_event_table(1)	Return active event table for particular queue.
    # @param queue_name [String] Name of the queue.
    # @return [String] false when queue already exist.
    def current_event_table(queue_name)
      adapter.select_value('SELECT pgq.current_event_table(?)', queue_name)
    end

    # pgq.register_consumer(2)	Subscribe consumer on a queue.
    # @param queue_name [String] Name of queue.
    # @param consumer_name [String] Name of consumer.
    # @return [Boolean] false when consumer already registered.
    def register_consumer(queue_name, consumer_name)
      adapter.select_value('SELECT pgq.register_consumer(?, ?)', queue_name, consumer_name) == 1
    end

    # pgq.register_consumer_at(3)	Extended registration, allows to specify tick_id.
    # @param queue_name [String] Name of queue.
    # @param consumer_name [String] Name of consumer.
    # @param tick_id [String] Tick ID.
    # @return [Boolean] false when consumer already exist.
    def register_consumer_at(queue_name, consumer_name, tick_id)
      adapter.select_value('SELECT pgq.register_consumer(?, ?, ?)', queue_name, consumer_name, tick_id) == 1
    end

    # pgq.unregister_consumer(2)	Unsubscribe consumer from the queue.
    # @param queue_name [String] Name of queue.
    # @param consumer_name [String] Name of consumer.
    # @return [Boolean] false when consumer does not exist.
    def unregister_consumer(queue_name, consumer_name)
      adapter.select_value('SELECT pgq.unregister_consumer(?, ?)', queue_name, consumer_name) == 1
    end

    # pgq.next_batch_info(2)	Makes next block of events active.
    # @param queue_name [String] Name of queue.
    # @param consumer_name [String] Name of consumer.
    # @return [Hash] keys: batch_id, cur_tick_id, prev_tick_id, cur_tick_time, prev_tick_time,
    #   cur_tick_event_seq, prev_tick_event_seq.
    #   all values are nil if there are no more events available.
    def next_batch_info(queue_name, consumer_name)
      adapter.select_one('SELECT * FROM pgq.next_batch_info(?, ?)', queue_name, consumer_name)
    end

    # pgq.next_batch(2)	Old function that returns just batch_id.
    # @param queue_name [String] Name of queue.
    # @param consumer_name [String] Name of consumer.
    # @return [Integer] Batch ID or NULL if there are no more events available.
    def next_batch(queue_name, consumer_name)
      adapter.select_value('SELECT pgq.next_batch(?, ?)', queue_name, consumer_name)
    end

    # pgq.next_batch_custom(5)	Makes next block of events active.
    # @param queue_name [String]	Name of the queue.
    # @param consumer_name [String]	Name of the consumer.
    # @param min_lag [String]	Consumer wants events older than that.
    #   @example "05:25:30" - 5 hours 25 minutes and 30 seconds.
    # @param min_count [Integer]	Consumer wants batch to contain at least this many events.
    # @param min_interval [String]	Consumer wants batch to cover at least this much time.
    #   @example "05:25:30" - 5 hours 25 minutes and 30 seconds.
    # @return [Hash] keys: batch_id, cur_tick_id, cur_tick_time, cur_tick_event_seq, prev_tick_id,
    #   prev_tick_time, prev_tick_event_seq.
    #   all values are nil if there are no more events available.
    def next_batch_custom(queue_name, consumer_name, min_lag, min_count, min_interval)
      adapter.select_one(
        'SELECT * FROM pgq.next_batch_custom(?, ?, ?::interval, ?, ?::interval)',
        queue_name, consumer_name, min_lag, min_count, min_interval
      )
    end

    # pgq.get_batch_events(1)	Get all events in batch.
    # @param batch_id [Integer]
    # @return [Array<Hash>] keys: ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
    #   ev_extra1, ev_extra2, ev_extra3, ev_extra4.
    def get_batch_events(batch_id)
      adapter.select_all('SELECT * FROM pgq.get_batch_events(?)', batch_id)
    end

    # pgq.get_batch_cursor(4)	Get events in batch using a cursor.
    # pgq.get_batch_cursor(3)	Get events in batch using a cursor.
    # @param batch_id [Integer]	ID of active batch.
    # @param cursor_name [String]	Name for new cursor.
    # @param quick_limit [Integer]	Number of events to return immediately.
    # @param extra_where [String,nil]	optional where clause to filter events.
    # @return [Array<Hash>] keys: ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
    #   ev_extra1, ev_extra2, ev_extra3, ev_extra4.
    def get_batch_cursor(batch_id, cursor_name, quick_limit, extra_where = nil)
      if extra_where.nil?
        adapter.select_all('SELECT * FROM pgq.get_batch_cursor(?, ?, ?)', batch_id, cursor_name, quick_limit)
      else
        adapter.select_all(
          'SELECT * FROM pgq.get_batch_cursor(?, ?, ?, ?)',
          batch_id, cursor_name, quick_limit, extra_where
        )
      end
    end

    # pgq.event_retry(3a)	Put the event into retry queue, to be processed again later.
    # pgq.event_retry(3b)	Put the event into retry queue, to be processed later again.
    # Only one of (retry_time, retry_seconds) must be provided.
    # @param batch_id [Integer]	ID of active batch..
    # @param event_id [Integer]	event id.
    # @param retry_time [String,Time,Date,nil] Time when the event should be put back into queue.
    # @param retry_seconds [Integer,nil] Time when the event should be put back into queue.
    # @return [Boolean] false when event already in retry queue.
    def event_retry(batch_id, event_id, retry_time: nil, retry_seconds: nil)
      if retry_time
        adapter.select_value('SELECT pgq.event_retry(?, ?, ?)', batch_id, event_id, retry_time) == 1
      else
        adapter.select_value('SELECT pgq.event_retry(?, ?, ?)', batch_id, event_id, retry_seconds) == 1
      end
    end

    # pgq.batch_retry(2)	Put whole batch into retry queue, to be processed again later.
    # @param batch_id [String] ID of active batch.
    # @param retry_seconds [Integer,nil] Time when the event should be put back into queue.
    # @return [Integer] number of events inserted.
    def batch_retry(batch_id, retry_seconds)
      adapter.select_value('SELECT pgq.batch_retry(?, ?)', batch_id, retry_seconds)
    end

    # pgq.finish_batch(1)	Closes a batch.
    # @param batch_id [String] ID of active batch.
    # @return [Boolean] false when batch not found.
    def finish_batch(batch_id)
      adapter.select_value('SELECT pgq.finish_batch(?)', batch_id) == 1
    end

    # pgq.get_queue_info(0)	Get info about all queues.
    # pgq.get_queue_info(1)	Get info about particular queue.
    # @param queue_name [String,nil] Name of queue.
    # @return [Hash,Array<Hash>] keys: queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
    #   queue_switch_time, queue_external_ticker, queue_ticker_paused, queue_ticker_max_count,
    #   queue_ticker_max_lag, queue_ticker_idle_period, ticker_lag, ev_per_sec, ev_new, last_tick_id.
    #   When queue_name provided return information about particular queue as Hash.
    #   Return information about all queues as Array otherwise.
    def get_queue_info(queue_name = nil)
      if queue_name.nil?
        adapter.select_all('SELECT * FROM pgq.get_queue_info()')
      else
        adapter.select_one('SELECT * FROM pgq.get_queue_info(?)', queue_name)
      end
    end

    # pgq.get_consumer_info(0)	Returns info about all consumers on all queues.
    # pgq.get_consumer_info(1)	Returns info about all consumers on single queue.
    # pgq.get_consumer_info(2)	Get info about particular consumer on particular queue.
    # @param queue_name [String,nil]
    # @param consumer_name [String,nil]
    # @return [Hash,Array<Hash>] keys: queue_name, consumer_name, lag, last_seen, last_tick,
    #   current_batch, next_tick, pending_events.
    #   When queue_name and consumer_name provided return information about particular consumer as Hash.
    #   When only queue_name provided return information about all consumers of a queue as Array.
    #   Return information about all consumers of all queues as Array otherwise.
    def get_consumer_info(queue_name = nil, consumer_name = nil)
      if queue_name.nil? && !consumer_name.nil?
        raise ArgumentError, 'queue_name must be provided if consumer_name is provided'
      end

      if queue_name.nil?
        adapter.select_all('SELECT * FROM pgq.get_consumer_info()')
      elsif consumer_name.nil?
        adapter.select_all('SELECT * FROM pgq.get_consumer_info(?)', queue_name)
      else
        adapter.select_one('SELECT * FROM pgq.get_consumer_info(?, ?)', queue_name, consumer_name)
      end
    end

    # pgq.version(0)	Returns version string for pgq.
    # @return [String]
    def version
      adapter.select_value('SELECT pgq.version()')
    end

    # pgq.get_batch_info(1)	Returns detailed info about a batch.
    # @param batch_id [Integer]
    # @return [Hash] keys: queue_name, consumer_name, batch_start, batch_end, prev_tick_id,
    #   tick_id, lag, seq_start, seq_end.
    def get_batch_info(batch_id)
      adapter.select_one('SELECT * FROM pgq.get_batch_info(?)', batch_id)
    end

    # pgq.force_tick(1) Simulate lots of events happening to force ticker to tick.
    # Should be called in loop, with some delay until last tick changes or too much time is passed.
    # Such function is needed because parallel calls of pgq.ticker() are
    # dangerous, and cannot be protected with locks as snapshot
    # is taken before locking.
    # @param queue_name [String] Name of queue.
    # @return [Integer] Tick ID.
    def force_tick(queue_name)
      adapter.select_value('SELECT pgq.force_tick(?)', queue_name)
    end

    # pgq.ticker(2) External ticker: Insert a tick with a particular tick_id and timestamp.
    # pgq.ticker(1) Check if tick is needed for the queue and insert it. For pgqadm usage.
    # @param queue_name [String] Name of queue.
    # @param tick_id [Integer,nil] Tick ID.
    # @return [Integer,nil] Tick ID or nil if no tick was done.
    def ticker(queue_name, tick_id = nil)
      if tick_id.nil?
        adapter.select_value('SELECT pgq.ticker(?)', queue_name)
      else
        adapter.select_value('SELECT pgq.ticker(?, ?::bigint)', queue_name, tick_id)
      end
    end
  end
end
