package org.infinispan.xsite.statetransfer;

/**
*
 */
public class SiteCachePair {
        private final String site;
        private final String cache;


        SiteCachePair(String cache, String site) {
            this.cache = cache;
            this.site = site;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SiteCachePair)) return false;

            SiteCachePair that = (SiteCachePair) o;

            if (cache != null ? !cache.equals(that.cache) : that.cache != null) return false;
            if (site != null ? !site.equals(that.site) : that.site != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = site != null ? site.hashCode() : 0;
            result = 31 * result + (cache != null ? cache.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "SiteCachePair{" +
                    "site='" + site + '\'' +
                    ", cache='" + cache + '\'' +
                    '}';
        }
    }