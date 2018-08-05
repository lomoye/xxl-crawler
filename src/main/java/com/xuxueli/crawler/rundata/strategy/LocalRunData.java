package com.xuxueli.crawler.rundata.strategy;

import com.xuxueli.crawler.exception.XxlCrawlerException;
import com.xuxueli.crawler.rundata.RunData;
import com.xuxueli.crawler.util.UrlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * lcoal run data
 *
 * @author xuxueli 2017-12-14 11:42:23
 */
public class LocalRunData extends RunData {
    private static Logger logger = LoggerFactory.getLogger(LocalRunData.class);

    private final AtomicLong totalAddCounter = new AtomicLong(0L);

    private final AtomicLong actualAddCounter = new AtomicLong(0L);

    private final AtomicLong repeatCounter = new AtomicLong(0L);

    private final AtomicLong takeCounter = new AtomicLong(0L);

    public AtomicLong getTotalAddCounter() {
        return totalAddCounter;
    }

    public AtomicLong getActualAddCounter() {
        return actualAddCounter;
    }

    public AtomicLong getRepeatCounter() {
        return repeatCounter;
    }

    public AtomicLong getTakeCounter() {
        return takeCounter;
    }

    // url
    private volatile LinkedBlockingQueue<String> unVisitedUrlQueue = new LinkedBlockingQueue<String>();     // 待采集URL池
    private volatile Set<String> visitedUrlSet = Collections.synchronizedSet(new HashSet<String>());        // 已采集URL池


    /**
     * url add
     * @param link
     */
    @Override
    public boolean addUrl(String link) {
        totalAddCounter.getAndIncrement();
        if (!UrlUtil.isUrl(link)) {
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link not valid: {}", link);
            return false; // check URL格式
        }
        if (visitedUrlSet.contains(link)) {
            repeatCounter.getAndIncrement();
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link repeate: {}", link);
            return false; // check 未访问过
        }
        if (unVisitedUrlQueue.contains(link)) {
            repeatCounter.getAndIncrement();
            logger.debug(">>>>>>>>>>> xxl-crawler addUrl fail, link visited: {}", link);
            return false; // check 未记录过
        }
        unVisitedUrlQueue.add(link);
        actualAddCounter.getAndIncrement();
        logger.info(">>>>>>>>>>> xxl-crawler addUrl success, link: {}", link);
        return true;
    }

    /**
     * url take
     * @return
     * @throws InterruptedException
     */
    @Override
    public String getUrl() {
        String link = null;
        try {
            link = unVisitedUrlQueue.take();
        } catch (InterruptedException e) {
            throw new XxlCrawlerException("LocalRunData.getUrl interrupted.");
        }
        if (link != null) {
            visitedUrlSet.add(link);
            takeCounter.getAndIncrement();
        }
        return link;
    }

    @Override
    public int getUrlNum() {
        return unVisitedUrlQueue.size();
    }

}
