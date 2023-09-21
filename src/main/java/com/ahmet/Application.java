package com.ahmet;

import com.ahmet.cluster.management.ServiceRegistry;
import com.ahmet.networking.WebServer;
import com.ahmet.search.UserSearchHandler;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class Application implements Watcher {
    private static Logger logger;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 9000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException {
        logger = LoggerFactory.getLogger(Application.class);
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;

        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistry coordinatorsServiceRegistry = new ServiceRegistry(zooKeeper, ServiceRegistry.COORDINATORS_SERVICE_REGISTRY);
        UserSearchHandler userSearchHandler = new UserSearchHandler(coordinatorsServiceRegistry);
        WebServer frontendWebServer = new WebServer(currentServerPort, userSearchHandler);
        frontendWebServer.startServer();
        logger.warn("Server started listening on port {}", currentServerPort);

        application.run();
        application.close();
        logger.warn("Disconnected from Zookeeper server, existing application");
    }

    private ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        if (Objects.requireNonNull(event.getType()) == Event.EventType.None) {
            if (Event.KeeperState.SyncConnected.equals(event.getState())) {
                logger.warn("Connected to Zookeeper Server");
            } else {
                synchronized (zooKeeper) {
                    logger.warn("Event: Disconnected from Zookeeper");
                    zooKeeper.notifyAll();
                }
            }
        }
    }
}
