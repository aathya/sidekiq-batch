module Sidekiq
  class Batch
    class Status
      attr_reader :bid

      def initialize(bid)
        @bid = bid
      end

      def join
        raise "Not supported"
      end

      def pending
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'pending') }.to_i
      end

      def created_at
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'created_at') }
      end

      def total
        Sidekiq.redis { |r| r.hget("BID-#{bid}", 'total') }.to_i
      end

      def parent_bid
        Sidekiq.redis { |r| r.hget("BID-#{bid}", "parent_bid") }
      end

      def failed_jobs
        Sidekiq.redis { |r| r.smembers("BID-#{bid}-failed") } || []
      end

      def all_jobs
        Sidekiq.redis { |r| r.smembers("BID-#{bid}-jids") } || []
      end

      def failures
        failed_jobs.to_i
      end

      def completed_jobs
        Sidekiq.redis { |r| r.smembers("BID-#{bid}-completed") } || []
      end

      def completed?
        'true' == Sidekiq.redis { |r| r.get("BID-#{bid}-callback_completed") }
      end

      def can_queue_callback?
        pending.zero? && failures.zero? && total == completed_jobs.size? && total == completed_jobs
      end

    end
  end
end
