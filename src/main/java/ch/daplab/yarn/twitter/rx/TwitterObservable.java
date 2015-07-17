package ch.daplab.yarn.twitter.rx;

import ch.daplab.config.Config;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by killerwhile on 22/04/15.
 */
public class TwitterObservable implements Observable.OnSubscribe<byte[]>, StatusListener {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterObservable.class);

    public static final String CONFIG_FILE = "/twitter.properties";

    private CountDownLatch countDownLatch = new CountDownLatch(1);

    private final AtomicBoolean close = new AtomicBoolean(false);

    // The actual Twitter stream. It's set up to collect raw JSON data
    private TwitterStream twitterStream;

    private volatile AtomicReference<Subscriber<? super byte[]>> subscriberRef = new AtomicReference<>(null);

    private final String oAuthConsumerKey;
    private final String oAuthConsumerSecret;
    private final String oAuthAccessToken;
    private final String oAuthAccessTokenSecret;

    public TwitterObservable(String oAuthConsumerKey, String oAuthConsumerSecret, String oAuthAccessToken, String oAuthAccessTokenSecret) {
        this.oAuthConsumerKey = oAuthConsumerKey;
        this.oAuthConsumerSecret = oAuthConsumerSecret;
        this.oAuthAccessToken = oAuthAccessToken;
        this.oAuthAccessTokenSecret = oAuthAccessTokenSecret;
    }

    @Override
    public void onException(Exception e) {

        LOG.error("Exception", e);

        Subscriber<? super byte[]> subscriber = subscriberRef.get();

        if (subscriber != null && !subscriber.isUnsubscribed()) {
            subscriber.onError(e);
        }
        internalClose();
    }

    @Override
    public void onStatus(Status status) {

        if (close.get()) {
            return;
        }

        Subscriber<? super byte[]> subscriber = subscriberRef.get();
        Preconditions.checkNotNull(subscriber, "Function OnSubscribe.call must be called first!");

        if (subscriber.isUnsubscribed()) {
            internalClose();
        } else {
            byte[] payload = TwitterObjectFactory.getRawJSON(status).getBytes();
            subscriber.onNext(payload);
        }
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int i) {

    }

    @Override
    public void onScrubGeo(long l, long l1) {

    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {

    }

    @Override
    public void call(Subscriber<? super byte[]> subscriber) {

        if (!subscriberRef.compareAndSet(null, subscriber)) {
            return;
        }

        try {
            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setOAuthConsumerKey(oAuthConsumerKey);
            cb.setOAuthConsumerSecret(oAuthConsumerSecret);
            cb.setOAuthAccessToken(oAuthAccessToken);
            cb.setOAuthAccessTokenSecret(oAuthAccessTokenSecret);
            cb.setJSONStoreEnabled(true);
            cb.setIncludeEntitiesEnabled(true);

            twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

            twitterStream.addListener(this);

            LOG.info("Starting to read from Twitter");

            twitterStream.sample();

            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted, letting it die", e);
            }

        } finally {
            internalClose();
        }
    }

    private void internalClose() {

        if (close.compareAndSet(false, true)) {
            if (twitterStream != null) {
                twitterStream.shutdown();
            }

            Subscriber<? super byte[]> subscriber = subscriberRef.get();
            if (subscriber != null && !subscriber.isUnsubscribed()) {
                subscriber.onCompleted();
                subscriber.unsubscribe();
            }

            countDownLatch.countDown();
        }
    }
}
