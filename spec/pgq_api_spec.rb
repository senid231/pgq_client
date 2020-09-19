# frozen_string_literal: true

RSpec.describe Pgq::API do
  let(:queue_name) { 'test_queue' }
  let(:consumer_name) { 'test_consumer' }

  def current_queue_names
    Pgq::API.adapter.select_all('SELECT queue_name FROM pgq.get_queue_info()').map { |r| r[:queue_name] }
  end

  before do
    current_queue_names.each do |queue_name|
      Pgq::API.drop_queue(queue_name, force: true)
    end
  end

  it 'creates and drops queue' do
    expect(current_queue_names).to eq []

    expect(
      Pgq::API.create_queue(queue_name)
    ).to eq(true)
    expect(current_queue_names).to eq [queue_name]

    expect(
      Pgq::API.create_queue(queue_name)
    ).to eq(false)
    expect(current_queue_names).to eq [queue_name]

    expect {
      Pgq::API.drop_queue('does_not_exist')
    }.to raise_error(ActiveRecord::StatementInvalid, /No such event queue/)
    expect(current_queue_names).to eq [queue_name]

    Pgq::API.drop_queue(queue_name)
    expect(current_queue_names).to eq []
  end

  it 'gets queue info' do
    queue_info_columns = [
      :queue_name, :queue_ntables, :queue_cur_table, :queue_rotation_period,
      :queue_switch_time, :queue_external_ticker, :queue_ticker_paused,
      :queue_ticker_max_count, :queue_ticker_max_lag, :queue_ticker_idle_period,
      :ticker_lag, :ev_per_sec, :ev_new, :last_tick_id
    ]

    expect(
      Pgq::API.get_queue_info
    ).to eq []

    Pgq::API.create_queue(queue_name)
    Pgq::API.create_queue('other')

    info_list = Pgq::API.get_queue_info
    expect(info_list).to be_a_kind_of(Array)
    expect(info_list.size).to eq 2
    expect(info_list.first).to be_a_kind_of(Hash)
    expect(info_list.first.keys).to match_array(queue_info_columns)
    expect(info_list.first).to be_a_kind_of(Hash)
    expect(info_list.first.keys).to match_array(queue_info_columns)

    queue_info = Pgq::API.get_queue_info(queue_name)
    expect(queue_info).to be_a_kind_of(Hash)
    expect(queue_info.keys).to match_array(queue_info_columns)
  end

  it 'inserts events' do
    Pgq::API.create_queue(queue_name)
    Pgq::API.register_consumer(queue_name, consumer_name)

    events = (1..101).map do |i|
      { type: "type_#{i}", data: { id: i, name: "event_#{i}" } }
    end

    events.each do |event|
      Pgq::API.insert_event(queue_name, event[:type], event[:data].to_json)
    end

    expect(
      Pgq::API.get_queue_info(queue_name)[:ev_new]
    ).to eq events.size - 1

    sleep 4 # wait when new tick can be created.
    expect(
      Pgq::API.ticker(queue_name)
    ).to be_a_kind_of(Integer)

    expect(
      Pgq::API.get_queue_info(queue_name)[:ev_new]
    ).to eq 0

    expect(
      Pgq::API.get_consumer_info(queue_name, consumer_name)[:pending_events]
    ).to eq events.size - 1
  end

  it 'get next batch' do
    Pgq::API.create_queue(queue_name)
    Pgq::API.register_consumer(queue_name, consumer_name)

    expect(
      Pgq::API.next_batch(queue_name, consumer_name)
    ).to be_nil

    events = (1..101).map do |i|
      { type: "type_#{i}", data: { id: i, name: "event_#{i}" } }
    end

    events.each do |event|
      Pgq::API.insert_event(queue_name, event[:type], event[:data].to_json)
    end

    sleep 4 # wait when new tick can be created.
    expect(
      Pgq::API.ticker(queue_name)
    ).to be_a_kind_of(Integer)

    batch_id = Pgq::API.next_batch(queue_name, consumer_name)
    expect(batch_id).to be_a_kind_of(Integer)
    expect(Pgq::API.next_batch(queue_name, consumer_name)).to eq(batch_id)
    expect(Pgq::API.next_batch(queue_name, consumer_name)).to eq(batch_id)

    batch_info = Pgq::API.get_batch_info(batch_id)
    expect(batch_info).to match(
                            queue_name: queue_name,
                            consumer_name: consumer_name,
                            batch_start: a_kind_of(Time),
                            batch_end: a_kind_of(Time),
                            prev_tick_id: a_kind_of(Integer),
                            tick_id: a_kind_of(Integer),
                            lag: a_kind_of(String),
                            seq_start: a_kind_of(Integer),
                            seq_end: a_kind_of(Integer)
                          )

    batch_events = Pgq::API.get_batch_events(batch_id)
    expect(batch_events.size).to eq(events.size)
    expect(batch_events).to match(
                              events.map do |ev|
                                {
                                  ev_id: a_kind_of(Integer),
                                  ev_retry: nil,
                                  ev_type: ev[:type],
                                  ev_data: ev[:data].to_json,
                                  ev_extra1: nil,
                                  ev_extra2: nil,
                                  ev_extra3: nil,
                                  ev_extra4: nil,
                                  ev_txid: a_kind_of(Integer),
                                  ev_time: a_kind_of(Time)
                                }
                              end
                            )

    expect(Pgq::API.finish_batch(batch_id)).to eq(true)
    expect(Pgq::API.next_batch(queue_name, consumer_name)).to be_nil
    expect(Pgq::API.finish_batch(batch_id)).to eq(false)
    expect { Pgq::API.get_batch_events(batch_id) }.to raise_error(
                                                        ActiveRecord::StatementInvalid,
                                                        /batch not found/
                                                      )
  end
end
