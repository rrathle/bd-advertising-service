package com.amazon.ata.advertising.service.businesslogic;

import com.amazon.ata.advertising.service.dao.ReadableDao;
import com.amazon.ata.advertising.service.model.AdvertisementContent;
import com.amazon.ata.advertising.service.model.EmptyGeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.GeneratedAdvertisement;
import com.amazon.ata.advertising.service.model.RequestContext;
import com.amazon.ata.advertising.service.targeting.TargetingEvaluator;
import com.amazon.ata.advertising.service.targeting.TargetingGroup;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;

/**
 * This class is responsible for picking the advertisement to be rendered.
 */
public class AdvertisementSelectionLogic {
    public static final boolean IMPLEMENTED_STREAMS = true;

    private static final Logger LOG = LogManager.getLogger(AdvertisementSelectionLogic.class);

    private final ReadableDao<String, List<AdvertisementContent>> contentDao;
    private final ReadableDao<String, List<TargetingGroup>> targetingGroupDao;
    private Random random = new Random();

    /**
     * Constructor for AdvertisementSelectionLogic.
     *
     * @param contentDao        Source of advertising content.
     * @param targetingGroupDao Source of targeting groups for each advertising content.
     */
    @Inject
    public AdvertisementSelectionLogic(ReadableDao<String, List<AdvertisementContent>> contentDao,
                                       ReadableDao<String, List<TargetingGroup>> targetingGroupDao) {
        this.contentDao = contentDao;
        this.targetingGroupDao = targetingGroupDao;
    }

    /**
     * Setter for Random class.
     *
     * @param random generates random number used to select advertisements.
     */
    public void setRandom(Random random) {
        this.random = random;
    }

    /**
     * Gets all of the content and metadata for the marketplace and determines which content can be shown.  Returns the
     * eligible content with the highest click through rate.  If no advertisement is available or eligible, returns an
     * EmptyGeneratedAdvertisement.
     *
     * @param customerId    - the customer to generate a custom advertisement for
     * @param marketplaceId - the id of the marketplace the advertisement will be rendered on
     * @return an advertisement customized for the customer id provided, or an empty advertisement if one could
     * not be generated.
     */
    public GeneratedAdvertisement selectAdvertisement(String customerId, String marketplaceId) {
        if (StringUtils.isEmpty(marketplaceId)) {
            LOG.warn("MarketplaceId cannot be null or empty. Returning empty ad.");
            return new EmptyGeneratedAdvertisement();
        }

        List<AdvertisementContent> contents = contentDao.get(marketplaceId);
        if (CollectionUtils.isEmpty(contents)) {
            LOG.warn("No advertisements found for marketplace: " + marketplaceId);
            return new EmptyGeneratedAdvertisement();
        }

        Map<String, List<TargetingGroup>> targetingGroupsByContent = contents.stream()
                .collect(Collectors.toMap(
                        AdvertisementContent::getContentId,
                        content -> targetingGroupDao.get(content.getContentId())
                ));

        RequestContext requestContext = new RequestContext(customerId, marketplaceId);
        TargetingEvaluator evaluator = new TargetingEvaluator(requestContext);

        Comparator<Double> descendingCTR = Comparator.reverseOrder();
        TreeMap<Double, AdvertisementContent> sortedAds = new TreeMap<>(descendingCTR);

        for (AdvertisementContent content : contents) {
            List<TargetingGroup> groups = targetingGroupsByContent.get(content.getContentId());

            if (groups == null) continue;

            // Find highest CTR among eligible targeting groups
            OptionalDouble bestCTR = groups.stream()
                    .filter(group -> evaluator.evaluate(group).isTrue())
                    .mapToDouble(TargetingGroup::getClickThroughRate)
                    .max();

            if (bestCTR.isPresent()) {
                sortedAds.put(bestCTR.getAsDouble(), content);
            }
        }

        if (sortedAds.isEmpty()) {
            return new EmptyGeneratedAdvertisement();
        }

        return new GeneratedAdvertisement(sortedAds.firstEntry().getValue());
    }



}

