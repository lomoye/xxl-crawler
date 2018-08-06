package com.xuxueli.crawler.loader;

import com.xuxueli.crawler.model.PageLoadInfo;
import com.xuxueli.crawler.util.JsoupUtil;
import org.jsoup.nodes.Document;

/**
 * jsoup page loader
 *
 * @author xuxueli 2017-12-28 00:29:49
 */
public class JsoupPageLoader extends PageLoader {

    @Override
    public Document load(PageLoadInfo pageLoadInfo) {
        return JsoupUtil.load(pageLoadInfo);
    }

}
