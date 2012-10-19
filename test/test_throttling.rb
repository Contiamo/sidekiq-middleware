require 'securerandom'
require 'helper'
require 'sidekiq/client'
require 'sidekiq/worker'
require 'sidekiq/processor'
require 'sidekiq-middleware'

class TestThrottling < MiniTest::Unit::TestCase
  describe "with running redis" do
    before do
      @boss = MiniTest::Mock.new
      @processor = ::Sidekiq::Processor.new(@boss)

      @ttl1_key = "throttling:3106320b02b0ed75eca363f0996f3063-1"
      @ttl2_key = "throttling:3106320b02b0ed75eca363f0996f3063-2"
      Celluloid.logger = nil

      Sidekiq.redis = REDIS
      Sidekiq.redis {|c| c.flushdb }
    end

    class ThrottlingWorker
      include Sidekiq::Worker
      sidekiq_options queue: :throttled_queue, throttle: 5

      def perform(x)
      end
    end

    it "should queue first message" do
      5.times { ThrottlingWorker.perform_async(10) }
      assert_equal 1, Sidekiq.redis { |c| c.llen('queue:throttled_queue') }
    end

    it "should schedule successive messages" do
      5.times { ThrottlingWorker.perform_async(10) }
      assert_equal 1, Sidekiq.redis { |c| c.zcard('schedule') }
    end

    it "should schedule a message if there is already a ttl" do
      Sidekiq.redis { |c| c.set @ttl1_key, 1 }
      5.times { ThrottlingWorker.perform_async(10) }
      assert_equal 1, Sidekiq.redis { |c| c.zcard('schedule') }
    end

    it "should queue a message if both ttls expire" do
      5.times { ThrottlingWorker.perform_async(10) }
      assert_equal 1, Sidekiq.redis { |c| c.llen('queue:throttled_queue') }

      # remove both ttl keys
      Sidekiq.redis { |c| c.del @ttl1_key, @ttl2_key }
      5.times { ThrottlingWorker.perform_async(10) }
      assert_equal 2, Sidekiq.redis { |c| c.llen('queue:throttled_queue') }
    end

    it "should be thread safe and only queue once during the throttle period" do
      threads = (1..25).map do
        Thread.new do
          ThrottlingWorker.perform_async(10)
        end
      end

      threads.each &:join
      assert_equal 1, Sidekiq.redis { |c| c.llen('queue:throttled_queue') }
    end
  end
end