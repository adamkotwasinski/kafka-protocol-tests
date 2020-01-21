package tests;

import org.junit.Before;

import helpers.Scraper;

public abstract class BaseTest {

    protected Scraper scraper;

    @Before
    public void setUp() {
        this.scraper = new Scraper();
        this.scraper.collectInitialMetrics();
    }

}
