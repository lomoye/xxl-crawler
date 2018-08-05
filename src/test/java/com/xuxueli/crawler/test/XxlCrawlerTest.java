package com.xuxueli.crawler.test;

import com.xuxueli.crawler.XxlCrawler;
import com.xuxueli.crawler.annotation.PageFieldSelect;
import com.xuxueli.crawler.annotation.PageSelect;
import com.xuxueli.crawler.parser.PageParser;
import com.xuxueli.crawler.rundata.strategy.LocalRunData;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 爬虫示例01：爬取页面数据并封装VO对象
 *
 * @author xuxueli 2017-10-09 19:48:48
 */
public class XxlCrawlerTest {

    @PageSelect(cssQuery = "body")
    public static class PageVo {

        @PageFieldSelect(cssQuery = ".blog-detail .article-detail .header")
        private String title;

        @PageFieldSelect(cssQuery = ".ui.horizontal.list>.item:eq(2)")
        private String read;

        private String link;
//        @PageFieldSelect(cssQuery = ".comment-content")
//        private List<String> comment;

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getRead() {
            return read;
        }

        public void setRead(String read) {
            this.read = read;
        }

        public String getLink() {
            return link;
        }

        public void setLink(String link) {
            this.link = link;
        }

        //        public List<String> getComment() {
//            return comment;
//        }
//
//        public void setComment(List<String> comment) {
//            this.comment = comment;
//        }

        @Override
        public String toString() {
            return "PageVo{" +
                    "title='" + title + '\'' +
                    ", read=" + read +
                    " }";
        }
    }

    public static void main(String[] args) {

        final AtomicInteger counter = new AtomicInteger(0);
        final List<PageVo> pageVos = new ArrayList<>();

        final Lock lock = new ReentrantLock();

        LocalRunData runData = new LocalRunData();

        XxlCrawler crawler = new XxlCrawler.Builder()
                .setRunData(runData)
                .setFailRetryCount(3)
                .setPauseMillis(100)
                .setUrls("https://my.oschina.net/xuxueli")
                .setWhiteUrlRegexs("https://my\\.oschina\\.net/xuxueli/blog/\\d+")
                .setThreadCount(3)
                .setPageParser(new PageParser<PageVo>() {
                    @Override
                    public void parse(Document html, Element pageVoElement, PageVo pageVo) {
                        // 解析封装 PageVo 对象
                        String pageUrl = html.baseUri();
                        System.out.println("标记：" + pageUrl + "：" + pageVo.toString());
                        counter.getAndIncrement();

                        lock.lock();
                        pageVo.setLink(pageUrl);
                        pageVos.add(pageVo);

                        lock.unlock();
                    }
                })
                .build();

        System.out.println("start");
        crawler.start(true);
        System.out.println("end");
        System.out.println("总共解析数:" + counter);
        System.out.println("总添加url数:" + runData.getTotalAddCounter());
        System.out.println("总实际添加url数:" + runData.getActualAddCounter());
        System.out.println("总共重复url数:" + runData.getRepeatCounter());
        System.out.println("总共取出url数:" + runData.getTakeCounter());

        pageVos.forEach(pageVo -> System.out.println(pageVo.getLink()));
    }

}
