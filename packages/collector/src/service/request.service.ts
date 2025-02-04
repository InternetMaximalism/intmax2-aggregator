import {
  QueueManager,
  type RequestingWithdrawal,
  WithdrawalGroupStatus,
  RequestManager,
  WithdrawalPrisma,
  WithdrawalStatus,
  config,
  logger,
  withdrawalPrisma,
  ClaimStatus,
} from "@intmax2-withdrawal-aggregator/shared";

export const fetchPendingRequests = async () => {
  const requestManager = RequestManager.getInstance(config.AGGREGATOR_TYPE);
  const processedUUIDs = await requestManager.getAllProcessedUUIDs();

  const baseQuery = {
    select: {
      uuid: true,
      createdAt: true,
    },
    where: {
      uuid: {
        notIn: processedUUIDs,
      },
    },
    orderBy: {
      createdAt: WithdrawalPrisma.SortOrder.asc,
    },
  };

  const pendingRequests =
    config.AGGREGATOR_TYPE === "withdrawal"
      ? await fetchWithdrawalRequests(baseQuery)
      : await fetchClaimRequests(baseQuery);

  return pendingRequests as Array<RequestingWithdrawal & { createdAt: Date }>;
};

const fetchWithdrawalRequests = async (baseQuery: WithdrawalPrisma.WithdrawalFindManyArgs) => {
  const statusQuery = {
    status: WithdrawalStatus.requested,
  };
  const pendingRequests = await withdrawalPrisma.withdrawal.findMany({
    ...baseQuery,
    where: {
      ...baseQuery.where,
      ...statusQuery,
    },
  });

  return pendingRequests;
};

const fetchClaimRequests = async (baseQuery: WithdrawalPrisma.ClaimFindManyArgs) => {
  const statusQuery = {
    status: ClaimStatus.requested,
  };
  const pendingRequests = await withdrawalPrisma.claim.findMany({
    ...baseQuery,
    where: {
      ...baseQuery.where,
      ...statusQuery,
    },
  });

  return pendingRequests;
};

export const createRequestGroup = async (group: RequestingWithdrawal[]) => {
  const requestManager = RequestManager.getInstance(config.AGGREGATOR_TYPE);
  const queueManager = QueueManager.getInstance(config.AGGREGATOR_TYPE);

  const now = new Date();

  const groupId = await requestManager.addGroup({
    requestingWithdrawals: group.map((withdrawal) => ({
      uuid: withdrawal.uuid,
    })),
    status: WithdrawalGroupStatus.PENDING,
    createdAt: now,
    updatedAt: now,
  });

  await queueManager.addJob("processBatch", { groupId });

  logger.debug(`Created ${config.AGGREGATOR_TYPE} group ${groupId}`);

  return groupId;
};
