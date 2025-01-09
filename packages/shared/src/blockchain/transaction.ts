import { BaseError, ContractFunctionRevertedError, type PublicClient } from "viem";
import { logger, sleep } from "../lib";
import type { ContractCallOptions, ContractCallParameters } from "../types";
import { getWalletClient } from "./wallet";

interface TransactionExecutionParams {
  ethereumClient: PublicClient;
  walletClientData: ReturnType<typeof getWalletClient>;
  contractCallParams: ContractCallParameters;
  contractCallOptions?: ContractCallOptions;
}

export const executeTransaction = async ({
  ethereumClient,
  walletClientData,
  contractCallParams,
  contractCallOptions,
}: TransactionExecutionParams) => {
  const { functionName, contractAddress, abi, account, args } = contractCallParams;
  const { nonce } = contractCallOptions ?? {};

  try {
    logger.info(`functionName: ${functionName}, args: ${args}, nonce: ${nonce}`);

    const { request } = await ethereumClient.simulateContract({
      address: contractAddress,
      abi,
      functionName,
      account,
      args,
      nonce,
    });

    const transactionHash = await walletClientData.walletClient.writeContract(request);
    logger.info(`${functionName} transaction initiated with hash: ${transactionHash}`);

    return {
      transactionHash,
      nonce: request.nonce,
    };
  } catch (error) {
    handleContractError(`${functionName} failed`, error);

    throw error;
  }
};

export const replacedTransaction = async ({
  ethereumClient,
  walletClientData,
  contractCallParams,
  contractCallOptions,
}: TransactionExecutionParams) => {
  logger.warn(`Transaction was replaced. Sending new transaction...`);

  try {
    const transactionResult = await executeTransaction({
      ethereumClient,
      walletClientData,
      contractCallParams,
      contractCallOptions,
    });

    return transactionResult;
  } catch (error) {
    logger.warn(
      `Failed to send replaced transaction: ${error instanceof Error ? error.message : "Unknown error"}`,
    );

    throw error;
  }
};

export const waitForTransactionConfirmation = async (
  ethereumClient: PublicClient,
  transactionHash: `0x${string}`,
  functionName: string,
  options?: {
    timeout?: number;
    maxRetries?: number;
    retryDelay?: number;
  },
) => {
  const { timeout, maxRetries = 3, retryDelay = 1000 } = options ?? {};
  let retryCount = 0;

  while (true) {
    try {
      const receipt = await ethereumClient.waitForTransactionReceipt({
        hash: transactionHash,
        timeout,
      });

      if (receipt.status !== "success") {
        throw new Error(`Transaction failed with status: ${receipt.status}`);
      }

      logger.info(`${functionName} transaction confirmed`);
      return receipt;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";

      // NOTE: Scroll Sepolia
      if (errorMessage.includes("could not be found")) {
        if (retryCount < maxRetries) {
          retryCount++;
          logger.info(
            `Transaction not found, retrying (${retryCount}/${maxRetries}) after ${retryDelay}ms delay...`,
          );
          await sleep(retryDelay);
          continue;
        }
      }

      logger.warn(`Failed to confirm ${functionName} transaction: ${errorMessage}`);
      throw error;
    }
  }
};

const handleContractError = (errorMessage: string, error: unknown) => {
  if (error instanceof BaseError) {
    const revertError = error.walk((e) => e instanceof ContractFunctionRevertedError);
    if (revertError instanceof ContractFunctionRevertedError) {
      const errorName = revertError.data?.errorName ?? "Unknown contract error";
      const shortMessage = revertError.shortMessage ?? "Unknown shortMessage";
      logger.warn(`${errorMessage}: Contract revert error - ${errorName} - ${shortMessage}`);
    } else {
      logger.warn(`${errorMessage}: Base error - ${error.message}`);
    }
  } else if (error instanceof Error) {
    logger.warn(`${errorMessage}: Unexpected error - ${error.message}`);
  } else {
    logger.warn(`${errorMessage}: Unknown error occurred`);
  }
};

export const handleApiContractError = (error: unknown) => {
  let errorMessage = "An internal server error occurred.";

  if (error instanceof BaseError) {
    const revertError = error.walk((e) => e instanceof ContractFunctionRevertedError);
    if (revertError instanceof ContractFunctionRevertedError) {
      const errorName = revertError.data?.errorName ?? "Unknown error";
      const shortMessage = revertError.shortMessage ?? "Unknown shortMessage";
      logger.warn(`Contract revert error - ${errorName} - ${shortMessage}`);
      errorMessage = `Error occurred during contract execution: ${errorName}`;
    } else {
      logger.warn(`Base error: ${error.message}`);
      errorMessage = `A base error occurred: ${error.message}`;
    }
  } else if (error instanceof Error) {
    logger.warn(`Unexpected error:, ${error.message}`);
    errorMessage = `An unexpected error occurred: ${error.message}`;
  } else {
    logger.warn("Unknown error occurred");
    errorMessage = "An unknown error occurred";
  }

  return errorMessage;
};
