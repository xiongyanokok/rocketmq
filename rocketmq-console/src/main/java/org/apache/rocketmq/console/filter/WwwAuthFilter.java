package org.apache.rocketmq.console.filter;

import com.google.common.base.Joiner;
import org.apache.tomcat.util.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * 认证过滤器.
 *
 * @author yuanyue
 */
public final class WwwAuthFilter implements Filter {

    private Logger logger = LoggerFactory.getLogger(WwwAuthFilter.class);

    private static final String FILE_SEPARATOR = System.getProperty("file.separator");

    private static final String AUTH_PREFIX = "Basic ";

    private static final String ROOT_IDENTIFY = "root";

    private static final String ROOT_DEFAULT_USERNAME = "root";

    private static final String ROOT_DEFAULT_PASSWORD = "root";

    private String rootUsername;

    private String rootPassword;


    @Override
    public void init(final FilterConfig filterConfig) throws ServletException {
        Properties props = new Properties();
        URL classLoaderURL = Thread.currentThread().getContextClassLoader().getResource("");
        if (null != classLoaderURL) {
            String configFilePath = Joiner.on(FILE_SEPARATOR).join(classLoaderURL.getPath(), "", "application.properties");
            try {
                props.load(new FileInputStream(configFilePath));
            } catch (final IOException ex) {
                logger.warn("Cannot found auth config file, use default auth config.");
            }
        }
        rootUsername = props.getProperty("root.username", ROOT_DEFAULT_USERNAME);
        rootPassword = props.getProperty("root.password", ROOT_DEFAULT_PASSWORD);
    }

    @Override
    public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;
        String authorization = httpRequest.getHeader("authorization");
        if (null != authorization && authorization.length() > AUTH_PREFIX.length()) {
            authorization = authorization.substring(AUTH_PREFIX.length(), authorization.length());
            if ((rootUsername + ":" + rootPassword).equals(new String(Base64.decodeBase64(authorization)))) {
                authenticateSuccess(httpResponse);
                chain.doFilter(httpRequest, httpResponse);
            } else {
                needAuthenticate(httpResponse);
            }
        } else {
            needAuthenticate(httpResponse);
        }
    }

    private void authenticateSuccess(final HttpServletResponse response) {
        response.setStatus(200);
        response.setHeader("Pragma", "No-cache");
        response.setHeader("Cache-Control", "no-store");
        response.setDateHeader("Expires", 0);
        response.setHeader("identify",  ROOT_IDENTIFY);
    }

    private void needAuthenticate(final HttpServletResponse response) {
        response.setStatus(401);
        response.setHeader("Cache-Control", "no-store");
        response.setDateHeader("Expires", 0);
        response.setHeader("WWW-authenticate", AUTH_PREFIX + "Realm=\"Elastic Job Console Auth\"");
    }

    @Override
    public void destroy() {
    }
}