import { type RequestingWithdrawal, config, logger } from "@intmax2-withdrawal-aggregator/shared";
import { differenceInMinutes } from "date-fns";
import { chunkArray } from "../lib/utils";
import { createRequestGroup, fetchPendingRequests } from "./request.service";

export const performJob = async (): Promise<void> => {
  logger.info(`Processing for ${config.AGGREGATOR_TYPE}`);

  const pendingRequests = await fetchPendingRequests();

  if (pendingRequests.length === 0) {
    logger.info("No pending requests found");
    return;
  }

  const shouldProcess = shouldProcessRequests(pendingRequests);
  if (!shouldProcess) {
    logger.info("Conditions not met for processing requests");
    return;
  }

  const requestGroups = chunkArray<RequestingWithdrawal>(
    pendingRequests,
    config.WITHDRAWAL_GROUP_SIZE,
  );

  const groupIds = await Promise.all(requestGroups.map(createRequestGroup));

  logger.info(
    `Successfully processed ${pendingRequests.length} ${config.AGGREGATOR_TYPE} and created ${groupIds.length} groups`,
  );
};

const shouldProcessRequests = (
  pendingRequests: Array<RequestingWithdrawal & { createdAt: Date }>,
) => {
  const oldestRequest = pendingRequests[0];
  const minutesSinceOldest = differenceInMinutes(new Date(), new Date(oldestRequest.createdAt));
  const hasEnoughRequests = pendingRequests.length >= config.WITHDRAWAL_MIN_BATCH_SIZE;
  const isOldEnough = minutesSinceOldest >= config.WITHDRAWAL_MIN_WAIT_MINUTES;

  logger.info(`shouldProcess hasEnoughRequests: ${hasEnoughRequests} isOldEnough: ${isOldEnough}`);

  return hasEnoughRequests || isOldEnough;
};
