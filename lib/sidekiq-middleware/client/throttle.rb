module Sidekiq
  module Middleware
    module Client
      class Throttle
        def call(worker_class, item, queue)
          throttle = worker_class.get_sidekiq_options['throttle']

          if throttle
            payload = item.clone
            payload.delete('jid')
            payload_hash = Digest::MD5.hexdigest(Sidekiq.dump_json(Hash[payload.sort]))

            ttl1_hash = "throttling:#{payload_hash}-1"
            ttl2_hash = "throttling:#{payload_hash}-2"
            result = Sidekiq.redis { |conn| conn.eval THROTTLE_SCRIPT, [ttl1_hash, ttl2_hash], [throttle] }

            case result.first
            when 'schedule'
              Sidekiq.redis do |conn|
                schedule_at = Time.now.to_f + result.last
                conn.zadd('schedule', schedule_at.to_s, Sidekiq.dump_json(item))
              end
            when 'queue'
              yield
            end
          else
            yield
          end
        end

        THROTTLE_SCRIPT = <<-EOS
          local ttl1 = redis.call('ttl', KEYS[1])
          local ttl2 = redis.call('ttl', KEYS[2])
          local throttle = ARGV[1]

          local mttl = ttl1 * ttl2

          if mttl == 1 then
            redis.call('setex', KEYS[1], throttle, 1)
            return { 'queue' }
          elseif mttl < 0 then
            if ttl1 == -1 then
              local expiry = throttle + ttl2
              redis.call('setex', KEYS[1], expiry, 1)
              return { 'schedule', expiry }
            else
              local expiry = throttle + ttl1
              redis.call('setex', KEYS[2], expiry, 1)
              return { 'schedule', expiry }
            end
          else
            return {}
          end
        EOS
      end
    end
  end
end
