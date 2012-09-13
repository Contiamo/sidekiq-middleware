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

            Sidekiq.redis do |conn|
              ttl1_hash = "#{payload_hash}-1"
              ttl2_hash = "#{payload_hash}-2"
              ttls = { "#{ttl1_hash}" => conn.ttl(ttl1_hash), "#{ttl2_hash}" => conn.ttl(ttl2_hash) }

              # continue if at least one ttl is -1
              if ttls.values.min == -1
                # if both values are -1
                ttl = if ttls.values.inject(&:*) == 1
                  # put job in queue immediately
                  yield
                  { 'hash' => ttls.keys.first, 'expire' => throttle }
                else
                  # find out which key expired
                  ttl_key = ttls.key -1
                  # get the ttl for the non-expired key
                  non_expired_ttl = ttls.values.inject(&:+) + 1
                  schedule_at = non_expired_ttl.to_f + Time.now.to_f
                  # schedule job for later execution
                  conn.zadd('schedule', schedule_at.to_s, Sidekiq.dump_json(item))
                  { 'hash' => "#{ttl_key}", 'expire' => non_expired_ttl + throttle }
                end

                conn.setex(ttl['hash'], ttl['expire'], 1)
              end
            end
          else
            yield
          end
        end
      end
    end
  end
end
