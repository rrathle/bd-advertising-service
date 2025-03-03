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
//        GeneratedAdvertisement generatedAdvertisement = new EmptyGeneratedAdvertisement();
//        if (StringUtils.isEmpty(marketplaceId)) {
//            LOG.warn("MarketplaceId cannot be null or empty. Returning empty ad.");
//        } else {
//            final List<AdvertisementContent> contents = contentDao.get(marketplaceId);
//
//            if (CollectionUtils.isNotEmpty(contents)) {
//                AdvertisementContent randomAdvertisementContent = contents.get(random.nextInt(contents.size()));
//                generatedAdvertisement = new GeneratedAdvertisement(randomAdvertisementContent);
//            }
//
//        }
//
//        return generatedAdvertisement;
            if (StringUtils.isEmpty(marketplaceId)) {
                System.out.println("WARNING: MarketplaceId cannot be null or empty. Returning empty ad.");
                return new EmptyGeneratedAdvertisement();
            }

            System.out.println("Retrieving advertisements for marketplace: " + marketplaceId);
            List<AdvertisementContent> contents = contentDao.get(marketplaceId);

            if (CollectionUtils.isEmpty(contents)) {
                System.out.println("WARNING: No advertisements found for marketplace: " + marketplaceId + ". Returning empty ad.");
                return new EmptyGeneratedAdvertisement();
            }

            // Fetch all targeting groups for the given advertisements
            System.out.println("Fetching targeting groups for " + contents.size() + " advertisements...");
            Map<String, List<TargetingGroup>> targetingGroupsByContent = contents.stream()
                    .collect(Collectors.toMap(
                            AdvertisementContent::getContentId,
                            content -> {
                                List<TargetingGroup> groups = targetingGroupDao.get(content.getContentId());
                                System.out.println("DEBUG: Content ID " + content.getContentId() + " has " + (groups != null ? groups.size() : 0) + " targeting groups.");
                                return groups;
                            }
                    ));

            // Create RequestContext once for evaluation
            RequestContext requestContext = new RequestContext(customerId, marketplaceId);
            TargetingEvaluator evaluator = new TargetingEvaluator(requestContext);

            // Filter eligible advertisements
            System.out.println("Filtering eligible advertisements...");
            List<AdvertisementContent> eligibleAds = contents.stream()
                    .filter(content -> {
                        List<TargetingGroup> contentTargetingGroups = targetingGroupsByContent.get(content.getContentId());
                        boolean isEligible = contentTargetingGroups != null && contentTargetingGroups.stream()
                                .anyMatch(group -> evaluator.evaluate(group).isTrue());

                        System.out.println("DEBUG: Content ID " + content.getContentId() + " eligibility: " + isEligible);
                        return isEligible;
                    })
                    .collect(Collectors.toList());

            // If no eligible ads, return empty advertisement
            if (eligibleAds.isEmpty()) {
                System.out.println("WARNING: No eligible advertisements found for customer " + customerId + " in marketplace " + marketplaceId + ". Returning empty ad.");
                return new EmptyGeneratedAdvertisement();
            }

            // Randomly select an advertisement from eligible ads
            AdvertisementContent selectedContent = eligibleAds.get(random.nextInt(eligibleAds.size()));
            System.out.println("SUCCESS: Selected advertisement ID: " + selectedContent.getContentId());
            return new GeneratedAdvertisement(selectedContent);
        }


    }

