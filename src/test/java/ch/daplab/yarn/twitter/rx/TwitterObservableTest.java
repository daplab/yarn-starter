package ch.daplab.yarn.twitter.rx;

import ch.daplab.config.Config;
import org.junit.Test;
import org.mockito.Mockito;
import rx.Observable;
import rx.Observer;

import java.io.IOException;
import java.util.Random;

public class TwitterObservableTest {

    private final Random r = new Random();

    @Test
    public void test() throws IOException {

        final int numberOfTweets = r.nextInt(100) + 100;

        Config config = Config.load(TwitterObservable.CONFIG_FILE);

        final TwitterObservable twitterObservable = new TwitterObservable(config.getProperty("oAuthConsumerKey"), config.getProperty("oAuthConsumerSecret"), config.getProperty("oAuthAccessToken"), config.getProperty("oAuthAccessTokenSecret"));

        Observer<byte[]> observerMock = Mockito.mock(Observer.class);

        Observable<byte[]> observable = Observable.create(twitterObservable);

        // block until onComplete is called.
        observable.limit(numberOfTweets).subscribe(observerMock);

        Mockito.verify(observerMock, Mockito.times(numberOfTweets)).onNext(Mockito.<byte[]>any());
        Mockito.verify(observerMock, Mockito.times(1)).onCompleted();
        Mockito.verify(observerMock, Mockito.never()).onError(Mockito.<Throwable>any());

    }
}
