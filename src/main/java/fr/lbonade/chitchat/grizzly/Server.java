package fr.lbonade.chitchat.grizzly;

import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by lbonade on 19/07/2014.
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    // TCP Host
    public static final String HOST = "localhost";
    // TCP port
    public static final int PORT = 7777;

    public static void main(String[] args) throws IOException {
        HttpServer http = new HttpServer();
        NetworkListener networkListener = new NetworkListener("sample-listener", "127.0.0.1", 9999);
        ThreadPoolConfig threadPoolConfig = ThreadPoolConfig
                .defaultConfig()
                .setCorePoolSize(2)
                .setMaxPoolSize(8);
        networkListener.getTransport().setWorkerThreadPoolConfig(threadPoolConfig);
        EventExecutorGroup executor = new DefaultEventExecutorGroup(10);
        try {
            http.addListener(networkListener);
            http.getServerConfiguration().addHttpHandler(new ThreadHandler(), "/chitchat/thread/*");
            http.getServerConfiguration().addHttpHandler(new LatestHandler(), "/chitchat/latest/*");
            http.getServerConfiguration().addHttpHandler(new SearchHandler(executor), "/chitchat/search");
            http.getServerConfiguration().addHttpHandler(new PostHandler(), "/chitchat/");

            http.start();
            System.out.println("Press any key to stop the server...");
            System.in.read();
            executor.shutdownGracefully();
        } catch (Exception e) {
            System.err.println(e);
        }
    }

}
