// SPDX-License-Identifier: Apache-2.0

import { aql, type DatabaseManagerInstance, type LoggerService, type ManagerConfig } from '@frmscoe/frms-coe-lib';
import { type OutcomeResult, type RuleConfig, type RuleRequest, type RuleResult } from '@frmscoe/frms-coe-lib/lib/interfaces';
import { unwrap } from '@frmscoe/frms-coe-lib/lib/helpers/unwrap';

export async function handleTransaction(
  req: RuleRequest,
  determineOutcome: (value: number, ruleConfig: RuleConfig, ruleResult: RuleResult) => RuleResult,
  ruleRes: RuleResult,
  loggerService: LoggerService,
  ruleConfig: RuleConfig,
  databaseManager: DatabaseManagerInstance<ManagerConfig>,
): Promise<RuleResult> {
  const context = `Rule-${ruleConfig?.id ? ruleConfig.id : '<unresolved>'} handleTransaction()`;
  const msgId = req.transaction.FIToFIPmtSts.GrpHdr.MsgId;

  loggerService.trace('Start - handle transaction', context, msgId);

  // Throw errors early if something we know we need is not provided - Guard Pattern
  if (!ruleConfig?.config?.bands || !ruleConfig.config.bands.length) {
    throw new Error('Invalid config provided - bands not provided or empty');
  }
  if (!ruleConfig.config.exitConditions) throw new Error('Invalid config provided - exitConditions not provided');
  if (!ruleConfig.config.parameters) throw new Error('Invalid config provided - parameters not provided');
  if (!ruleConfig.config.parameters.maxQueryRange) throw new Error('Invalid config provided - maxQueryRange parameter not provided');
  if (!req.DataCache?.dbtrAcctId) throw new Error('Data Cache does not have required dbtrAcctId');

  // Step 1: Early exit conditions

  loggerService.trace('Step 1 - Early exit conditions', context, msgId);

  const UnsuccessfulTransaction = ruleConfig.config.exitConditions.find((b: OutcomeResult) => b.subRuleRef === '.x00');

  if (req.transaction.FIToFIPmtSts.TxInfAndSts.TxSts !== 'ACCC') {
    if (UnsuccessfulTransaction === undefined) throw new Error('Unsuccessful transaction and no exit condition in config');

    return {
      ...ruleRes,
      reason: UnsuccessfulTransaction.reason,
      subRuleRef: UnsuccessfulTransaction.subRuleRef,
    };
  }

  // Step 2: Query Setup

  loggerService.trace('Step 2 - Query setup for creditor', context, msgId);

  const currentPacs002TimeFrame = req.transaction.FIToFIPmtSts.GrpHdr.CreDtTm;
  const creditorAccountId = `accounts/${req.DataCache.cdtrAcctId}`;
  const creditorAccIdAql = aql`${creditorAccountId}`;
  const maxQueryRange: number = ruleConfig.config.parameters.maxQueryRange as number;
  const baselineQueryRange: number = (ruleConfig.config.parameters.baselineQueryRange as number) || maxQueryRange * 5;
  const maxQueryRangeAql = aql` AND DATE_TIMESTAMP(${currentPacs002TimeFrame}) - DATE_TIMESTAMP(pacs002.CreDtTm) <= ${maxQueryRange}`;

  // Step 3: Query past transactions for baseline activity calculation (larger timeframe)
  loggerService.trace('Step 3 - Baseline query setup for creditor', context, msgId);

  loggerService.trace('Step 3 - Baseline query setup for creditor', context, msgId);

  const baselineQueryString = aql`
    FOR pacs002 IN transactionRelationship
      FILTER pacs002._from == ${creditorAccIdAql}
      AND pacs002.TxTp == 'pacs.002.001.12'
      AND DATE_TIMESTAMP(${currentPacs002TimeFrame}) - DATE_TIMESTAMP(pacs002.CreDtTm) <= ${baselineQueryRange}
      COLLECT WITH COUNT INTO length
    RETURN length`;

  // Execute baseline query and assert the result as an array of numbers
  const baselineTransactions = (await databaseManager._pseudonymsDb.query(baselineQueryString).batches.all()) as number[];

  if (!Array.isArray(baselineTransactions) || baselineTransactions.length === 0) {
    throw new Error('Data error: irretrievable baseline transaction history or no transactions found.');
  }

  // Unwrap and calculate baseline
  const baselineCount = baselineTransactions[0] / (baselineQueryRange / maxQueryRange);

  if (baselineCount == null || typeof baselineCount !== 'number') {
    throw new Error('Data error: invalid baseline count type or null value.');
  }

  loggerService.trace('Step 4 - Query for current transactions for creditor', context, msgId);

  // Query for current transactions within the specific timeframe
  const queryString = aql`
  FOR pacs002 IN transactionRelationship
    FILTER pacs002._from == ${creditorAccIdAql}
    AND pacs002.TxTp == 'pacs.002.001.12'
    ${maxQueryRangeAql}
    AND pacs002.CreDtTm <= ${currentPacs002TimeFrame}
    COLLECT WITH COUNT INTO length
  RETURN length`;

  // Execute query for current transactions
  const numberOfRecentTransactions = await (await databaseManager._pseudonymsDb.query(queryString)).batches.all();
  const count = unwrap(numberOfRecentTransactions);

  if (count == null) {
    throw new Error('Data error: irretrievable transaction history');
  }

  if (typeof count !== 'number') {
    throw new Error('Data error: query result type mismatch - expected a number');
  }

  // Step 5: Determine if there's a surge in activity for the creditor
  const surgeThresholdMultiplier = (ruleConfig.config.parameters.surgeThresholdMultiplier as number) || 2; // Surge threshold multiplier (e.g., 2x normal activity)
  const isSurge = count > baselineCount * surgeThresholdMultiplier;

  loggerService.trace('End - handle transaction', context, msgId);

  // Step 6: Return the outcome based on whether a surge is detected
  if (isSurge) {
    return determineOutcome(1, ruleConfig, ruleRes);
  } else {
    return determineOutcome(0, ruleConfig, ruleRes);
  }
}
