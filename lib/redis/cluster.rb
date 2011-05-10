require 'redis'
require './crc16.rb'

# TODO
# ability to set max number of open links to Redis nodes

RedisClusterSlots = 4096

class Redis
    class Cluster
        def initialize(entrypoints=[])
            @entrypoints = entrypoints
            @slots = {} # Slot -> Instance map
            @links = {} # Instance -> Redis handle map
        end

        # This method, given a Redis command, will try to guess the right
        # slot. It does not need to return the right slot, as redirection
        # will fix that automatically.
        def guess_hashslot(args)
            return nil
        end

        def call_with_slot(slot,args)
            slot = guess_hashslot(nil) if !slot
            if slot and @slots[slot]
                instance = @slots[slot]
            else
                instance = @entrypoints[rand(@entrypoints.length)]
            end
            puts "Using instance #{instance}"
            if @links[instance] == nil
                host,port = instance.split(':')
                link = Redis.new(:host => host, :port => port)
                @links[instance] = link
            end
            puts "Link: #{@links[instance]}"
            begin
                reply = @links[instance].send(*args)
            rescue
                err = $!.to_s
                if err[0..4] == 'MOVED'
                    parts = err.split(" ")
                    slot = parts[1].to_i
                    instance = parts[2]
                    @slots[slot] = instance
                    puts "Moved into #{slot} (#{instance})"
                    return call_with_slot(slot,args)
                else
                    raise $!
                end
            end
            puts reply
        end

        def method_missing(*args)
            call_with_slot(nil,args)
        end
    end
end

cluster = Redis::Cluster.new(%w{127.0.0.1:6379 127.0.0.1:6380 127.0.0.1:6381})
cluster.incr('a')
