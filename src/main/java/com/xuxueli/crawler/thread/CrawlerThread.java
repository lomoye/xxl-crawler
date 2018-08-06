package com.xuxueli.crawler.thread;

import com.xuxueli.crawler.XxlCrawler;
import com.xuxueli.crawler.annotation.PageFieldSelect;
import com.xuxueli.crawler.annotation.PageSelect;
import com.xuxueli.crawler.conf.XxlCrawlerConf;
import com.xuxueli.crawler.exception.XxlCrawlerException;
import com.xuxueli.crawler.model.PageLoadInfo;
import com.xuxueli.crawler.util.FieldReflectionUtil;
import com.xuxueli.crawler.util.JsoupUtil;
import com.xuxueli.crawler.util.UrlUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * crawler thread
 *
 * @author xuxueli 2017-10-10 10:58:19
 */
public class CrawlerThread implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(CrawlerThread.class);

    private XxlCrawler crawler;
    private boolean running;
    private boolean toStop;

    public CrawlerThread(XxlCrawler crawler) {
        this.crawler = crawler;
        this.running = true;
        this.toStop = false;
    }

    public void toStop() {
        this.toStop = true;
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void run() {

        while (!toStop) {
            try {

                // ------- url ----------
                running = false;
                crawler.tryFinish();
                String link = crawler.getRunData().getUrl();
                running = true;
                logger.info(">>>>>>>>>>> xxl crawler, process link : {}", link);
                if (!UrlUtil.isUrl(link)) {
                    continue;
                }

                // failover
                for (int i = 0; i < (1 + crawler.getRunConf().getFailRetryCount()); i++) {
                    boolean ret = process(link);
                    if (crawler.getRunConf().getPauseMillis() > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(crawler.getRunConf().getPauseMillis());
                        } catch (InterruptedException e) {
                            logger.info(">>>>>>>>>>> xxl crawler thread is interrupted. 2{}", e.getMessage());
                        }
                    }
                    if (ret) {
                        break;
                    }
                }

            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    logger.info(">>>>>>>>>>> xxl crawler thread is interrupted. {}", e.getMessage());
                } else if (e instanceof XxlCrawlerException) {
                    logger.info(">>>>>>>>>>> xxl crawler thread {}", e.getMessage());
                } else {
                    logger.error(e.getMessage(), e);
                }
            }

        }
    }

    private boolean process(String link) throws IllegalAccessException, InstantiationException {
        // ------- html ----------
        String userAgent = crawler.getRunConf().getUserAgentList().size() > 1
                ? crawler.getRunConf().getUserAgentList().get(new Random().nextInt(crawler.getRunConf().getUserAgentList().size()))
                : crawler.getRunConf().getUserAgentList().size() == 1 ? crawler.getRunConf().getUserAgentList().get(0) : null;
        Proxy proxy = null;
        if (crawler.getRunConf().getProxyMaker() != null) {
            proxy = crawler.getRunConf().getProxyMaker().make();
        }

        PageLoadInfo pageLoadInfo = new PageLoadInfo();
        pageLoadInfo.setUrl(link);
        pageLoadInfo.setParamMap(crawler.getRunConf().getParamMap());
        pageLoadInfo.setCookieMap(crawler.getRunConf().getCookieMap());
        pageLoadInfo.setHeaderMap(crawler.getRunConf().getHeaderMap());
        pageLoadInfo.setUserAgent(userAgent);
        pageLoadInfo.setReferrer(crawler.getRunConf().getReferrer());
        pageLoadInfo.setIfPost(crawler.getRunConf().isIfPost());
        pageLoadInfo.setTimeoutMillis(crawler.getRunConf().getTimeoutMillis());
        pageLoadInfo.setProxy(proxy);

        // pre + load + post
        crawler.getRunConf().getPageParser().preLoad(pageLoadInfo);
        Document html = crawler.getRunConf().getPageLoader().load(pageLoadInfo);
        crawler.getRunConf().getPageParser().postLoad(html);

        if (html == null) {
            return false;
        }

        // ------- child link list (FIFO队列,广度优先) ----------
        if (crawler.getRunConf().isAllowSpread()) {     // limit child spread
            if (!crawler.getRunConf().isAllowTargetUrlSpread() && crawler.getRunConf().validTargetUrl(link)) {
                logger.info(">>>>>>>>>>> {} not allowed to spread", link);
            } else {
                Set<String> links = JsoupUtil.findLinks(html);
                if (links != null && links.size() > 0) {
                    for (String item : links) {
                        if (!crawler.getRunConf().validWhiteUrl(item)) {      // limit unvalid-child spread
                            continue;
                        }
                        crawler.getRunData().addUrl(item);
                    }
                }
            }

        }

        // ------- pagevo ----------
        if (!crawler.getRunConf().validTargetUrl(link)) {     // limit unvalid-page parse, only allow spread child
            return true;
        }

        // pagevo class-field info
        Type[] pageVoClassTypes = ((ParameterizedType) crawler.getRunConf().getPageParser().getClass().getGenericSuperclass()).getActualTypeArguments();
        Class pageVoClassType = (Class) pageVoClassTypes[0];

        PageSelect pageVoSelect = (PageSelect) pageVoClassType.getAnnotation(PageSelect.class);
        String pageVoCssQuery = (pageVoSelect != null && pageVoSelect.cssQuery().trim().length() > 0) ? pageVoSelect.cssQuery() : "html";

        // pagevo document 2 object
        Elements pageVoElements = html.select(pageVoCssQuery);

        if (pageVoElements != null && pageVoElements.hasText()) {
            for (Element pageVoElement : pageVoElements) {

                Object pageVo = pageVoClassType.newInstance();

                Field[] fields = pageVoClassType.getDeclaredFields();
                if (fields != null) {
                    for (Field field : fields) {
                        if (Modifier.isStatic(field.getModifiers())) {
                            continue;
                        }


                        // field origin value
                        PageFieldSelect fieldSelect = field.getAnnotation(PageFieldSelect.class);
                        String cssQuery = null;
                        XxlCrawlerConf.SelectType selectType = null;
                        String selectVal = null;
                        if (fieldSelect != null) {
                            cssQuery = fieldSelect.cssQuery();
                            selectType = fieldSelect.selectType();
                            selectVal = fieldSelect.selectVal();
                        }
                        if (cssQuery == null || cssQuery.trim().length() == 0) {
                            continue;
                        }

                        // field value
                        Object fieldValue = null;

                        if (field.getGenericType() instanceof ParameterizedType) {
                            ParameterizedType fieldGenericType = (ParameterizedType) field.getGenericType();
                            if (fieldGenericType.getRawType().equals(List.class)) {

                                //Type gtATA = fieldGenericType.getActualTypeArguments()[0];
                                Elements fieldElementList = pageVoElement.select(cssQuery);
                                if (fieldElementList != null && fieldElementList.size() > 0) {

                                    List<Object> fieldValueTmp = new ArrayList<Object>();
                                    for (Element fieldElement : fieldElementList) {

                                        String fieldElementOrigin = JsoupUtil.parseElement(fieldElement, selectType, selectVal);
                                        if (fieldElementOrigin == null || fieldElementOrigin.length() == 0) {
                                            continue;
                                        }
                                        try {
                                            fieldValueTmp.add(FieldReflectionUtil.parseValue(field, fieldElementOrigin));
                                        } catch (Exception e) {
                                            logger.error(e.getMessage(), e);
                                        }
                                    }

                                    if (fieldValueTmp.size() > 0) {
                                        fieldValue = fieldValueTmp;
                                    }
                                }
                            }
                        } else {

                            Elements fieldElements = pageVoElement.select(cssQuery);
                            String fieldValueOrigin = null;
                            if (fieldElements != null && fieldElements.size() > 0) {
                                fieldValueOrigin = JsoupUtil.parseElement(fieldElements.get(0), selectType, selectVal);
                            }

                            if (fieldValueOrigin == null || fieldValueOrigin.length() == 0) {
                                continue;
                            }

                            try {
                                fieldValue = FieldReflectionUtil.parseValue(field, fieldValueOrigin);
                            } catch (Exception e) {
                                logger.error(e.getMessage(), e);
                            }
                        }

                        if (fieldValue != null) {
                            field.setAccessible(true);
                            field.set(pageVo, fieldValue);
                        }
                    }
                }

                // pagevo output
                crawler.getRunConf().getPageParser().parse(html, pageVoElement, pageVo);
            }
        }

        return true;
    }

}