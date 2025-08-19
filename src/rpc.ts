import { JSONRPC, JSONRPCID, JSONRPCServer, JSONRPCParams } from "json-rpc-2.0";
import { backOff } from "exponential-backoff";
import { ccc } from "@ckb-ccc/core";
import { cccA } from "@ckb-ccc/core/advanced";
import _ from "lodash";

import {
  dbConnection,
  funder,
  incentivePercent,
  lockedSeconds,
  commitingSeconds,
  signerQueue,
  udtCellDeps,
  udtName,
  udtInfo,
  udtScript,
} from "./env";
import {
  buildKey,
  calculateBidUdts,
  cancelAllCommitingCells,
  env,
  epoch_timestamp,
  fetchFeeRate,
  txExternalKey,
  Logger,
  KEY_LIVE_CELLS,
  KEY_LOCKED_CELLS,
  KEY_COMMITING_CELLS,
  KEY_PENDING_TXS,
  KEY_PREFIX_TX,
  KEY_PREFIX_SIGNED_TX,
} from "./utils";

export const rpc = new JSONRPCServer();

rpc.addMethodAdvanced("initiate", async (request) => {
  const params = buildParams(request.params);
  return buildResponse(params.c, await initiate(params), request.id || null);
});

rpc.addMethodAdvanced("confirm", async (request) => {
  const params = buildParams(request.params);
  return buildResponse(params.c, await confirm(params), request.id || null);
});

type Case = "camel" | "snake";

interface Params {
  c: Case;
  tx: ccc.Transaction;
  others: any[];
}

interface Result {
  result?: any;
  error?: { code: number; message: string; data?: any };
}

function buildParams(params?: JSONRPCParams): Params {
  let c = "camel" as Case;
  if (!_.isArray(params)) {
    throw new Error("Params must be an array!");
  }
  if (params[params.length - 1] === "snake") {
    c = "snake";
    params.pop();
  }
  const txData = params.shift();
  if (!_.isObject(txData)) {
    throw new Error("The first element in params must be the tx object!");
  }
  if (_.has(txData, "outputs_data")) {
    c = "snake";
  }
  // TODO: validate input parameters
  const tx =
    c === "camel"
      ? ccc.Transaction.from(txData)
      : cccA.JsonRpcTransformers.transactionTo(txData as any);
  return {
    c,
    tx,
    others: params,
  };
}

function buildTx(c: Case, tx: ccc.Transaction) {
  if (c === "camel") {
    return tx;
  } else {
    return cccA.JsonRpcTransformers.transactionFrom(tx);
  }
}

function buildResponse(c: Case, result: Result, id: JSONRPCID | null) {
  const base = {
    jsonrpc: JSONRPC,
    id: id || null,
  };
  if (result.error !== undefined) {
    return Object.assign({}, base, {
      error: result.error,
    });
  } else {
    return Object.assign({}, base, {
      result: transformResultCase(c, result.result),
    });
  }
}

function transformResultCase(c: Case, result?: any): any {
  if (c === "camel") {
    return snakeToCamel(result);
  } else {
    return camelToSnake(result);
  }
}

function camelToSnake(obj: any) {
  return _.transform(obj, (result: any, value, key: string) => {
    const snakeKey = _.snakeCase(key);
    result[snakeKey] = _.isObject(value) ? camelToSnake(value) : value;
  });
}

function snakeToCamel(obj: any) {
  return _.transform(obj, (result: any, value, key: string) => {
    const snakeKey = _.camelCase(key);
    result[snakeKey] = _.isObject(value) ? snakeToCamel(value) : value;
  });
}

const ERROR_CODE_INVALID_INPUT = 2001;
const ERROR_CODE_SERVER = 2002;

<<<<<<< HEAD
async function initiate(params: Params): Promise<Result> {
  let tx = params.tx;
=======
async function initiate(params: any): Promise<Result> {
  // TODO: validate input parameters
  let tx = ccc.Transaction.from(params[0]);
  console.log("tx", tx);
  console.log("params", params);
>>>>>>> dbc4fae (chore: log params)

  const currentTimestamp = epoch_timestamp();
  const expiredTimestamp = (
    parseInt(currentTimestamp) + lockedSeconds
  ).toString();

  {
    const funderScript = (await funder.getAddressObjSecp256k1()).script;
    const collectingScript = (
      await ccc.Address.fromString(
        env("COLLECTING_POOL_ADDRESS"),
        funder.client,
      )
    ).script;

    for (const input of tx.inputs) {
      const cell = await input.getCell(funder.client);
      if (
        cell.cellOutput.lock.eq(funderScript) ||
        cell.cellOutput.lock.eq(collectingScript)
      ) {
        return {
          error: {
            code: ERROR_CODE_INVALID_INPUT,
            message: "Input cells uses invalid lock script!",
          },
        };
      }
    }
  }

  const inputCapacity = await tx.getInputsCapacity(funder.client);
  const outputCapacity = tx.getOutputsCapacity();

  if (inputCapacity >= outputCapacity) {
    return {
      error: {
        code: ERROR_CODE_INVALID_INPUT,
        message: "Input ckbytes are enough for output ckbytes!",
      },
    };
  }
  const bidTokensNoFee = outputCapacity - inputCapacity;

  const priceStr = await dbConnection.get(`PRICE:${udtName}`);
  if (priceStr === null || priceStr === undefined) {
    return {
      error: {
        code: ERROR_CODE_SERVER,
        message: `${udtInfo.human} price unknown!`,
      },
    };
  }
  const udtPricePerCkb = ccc.fixedPointFrom(priceStr, 6);

  const indices = params.others[0];
  for (const i of indices) {
    if (i < 0 || i >= tx.outputs.length || !tx.outputs[i].type?.eq(udtScript)) {
      return {
        error: {
          code: ERROR_CODE_SERVER,
          message: `Invalid indices!`,
        },
      };
    }
  }
  const availableUdtBalance = tx.outputsData.reduce((acc, data, i) => {
    if (!indices.includes(i)) {
      return acc;
    }
    return acc + ccc.udtBalanceFrom(data);
  }, ccc.numFrom(0));

  // The estimation here is that the user should always be available to
  // trade one more CKBytes, so as to cover for fees. The actual charged
  // fees will be calculated below and are typically much less than 1 CKB.
  const MAXIMUM_FEE = ccc.fixedPointFrom("1");
  const requestedCapacity = bidTokensNoFee + MAXIMUM_FEE;
  const estimateAskTokens = calculateBidUdts(
    udtPricePerCkb,
    incentivePercent,
    requestedCapacity,
  );
  if (availableUdtBalance <= estimateAskTokens) {
    return {
      error: {
        code: ERROR_CODE_INVALID_INPUT,
        message: `At least ${estimateAskTokens} UDT tokens must be available to convert to CKB!`,
      },
    };
  }

  let availableCapacity = 0n;
  // Lock one or more cells to provide ckbytes for current tx
  const capacityCellStartIndex = tx.outputs.length;
  while (availableCapacity < requestedCapacity) {
    const outPointBytes = await (dbConnection as any).lockCell(
      KEY_LIVE_CELLS,
      KEY_LOCKED_CELLS,
      KEY_COMMITING_CELLS,
      currentTimestamp,
      expiredTimestamp,
    );
    if (!outPointBytes) {
      return {
        error: {
          code: ERROR_CODE_SERVER,
          message: "No live cell is available for converting!",
        },
      };
    }
    const outPoint = ccc.OutPoint.fromBytes(outPointBytes);
    const cell = await funder.client.getCellLive(outPoint, true);
    if (cell === null || cell === undefined) {
      return {
        error: {
          code: ERROR_CODE_SERVER,
          message: "Server data mismatch!",
        },
      };
    }

    tx.inputs.push(
      ccc.CellInput.from({
        previousOutput: outPoint,
      }),
    );
    // We will do the actual CKB / UDT manipulation later when we can calculate the fee
    tx.outputs.push(cell.cellOutput);
    tx.outputsData.push(cell.outputData);

    availableCapacity += cell.capacityFree;
  }
  tx = await funder.prepareTransaction(tx);
  tx.addCellDeps(udtCellDeps);

  // Calculate the fee to build final ask / bid tokens
  const feeRate = await fetchFeeRate(funder.client);
  const fee = tx.estimateFee(feeRate);

  const bidTokens = bidTokensNoFee + fee;
  const askTokens = calculateBidUdts(
    udtPricePerCkb,
    incentivePercent,
    bidTokens,
  );

  // Modify locked cells to charge +bidTokens+ CKBytes, collect +askTokens+ UDTs
  {
    tx.outputsData[capacityCellStartIndex] = ccc.hexFrom(
      ccc.numLeToBytes(
        ccc.udtBalanceFrom(tx.outputsData[capacityCellStartIndex]) + askTokens,
        16,
      ),
    );
    let charged = 0n;
    for (
      let i = capacityCellStartIndex;
      i < tx.outputs.length && charged <= bidTokens;
      i++
    ) {
      const cell = ccc.Cell.from({
        previousOutput: ccc.OutPoint.from({
          txHash: tx.hash(),
          index: i,
        }),
        cellOutput: tx.outputs[i],
        outputData: tx.outputsData[i],
      });
      let currentCharged = cell.capacityFree;
      if (currentCharged > bidTokens - charged) {
        currentCharged = bidTokens - charged;
      }
      tx.outputs[i].capacity -= currentCharged;
      charged += currentCharged;
    }
  }

  // Charge +askTokens+ UDTs from output cells indices by +indices+
  let charged = 0n;
  for (const i of indices) {
    if (charged >= askTokens) {
      break;
    }
    const available = ccc.udtBalanceFrom(tx.outputsData[i]);
    let currentCharged = askTokens - charged;
    if (currentCharged > available) {
      currentCharged = available;
    }
    const left = available - currentCharged;
    tx.outputsData[i] = ccc.hexFrom(ccc.numLeToBytes(left, 16));
    charged += currentCharged;
  }
  if (charged < askTokens) {
    return {
      error: {
        code: ERROR_CODE_INVALID_INPUT,
        message: `At least ${askTokens} UDT tokens must be available to convert to CKB!`,
      },
    };
  }

  // A larger EX value is safer, and won't cost us too much here.
  await dbConnection.setex(
    buildKey(KEY_PREFIX_TX, txExternalKey(tx)),
    lockedSeconds * 2,
    ccc.hexFrom(tx.toBytes()),
  );

  return {
    result: {
      valid_until: new Date(parseInt(expiredTimestamp) * 1000).toISOString(),
      transaction: buildTx(params.c, tx),
      ask_tokens: askTokens,
      bid_tokens: bidTokens,
    },
  };
}

<<<<<<< HEAD
async function confirm(params: Params): Promise<Result> {
  let tx = params.tx;
=======
async function confirm(params: any): Promise<Result> {
  // TODO: validate input parameters
  console.log("params", params);
  console.log("params[0]", params[0]);
  let tx = ccc.Transaction.from(params[0]);
>>>>>>> dbc4fae (chore: log params)
  if (tx.inputs.length === 0) {
    return {
      error: {
        code: ERROR_CODE_INVALID_INPUT,
        message: "Transaction has no inputs!",
      },
    };
  }

  const currentTimestamp = epoch_timestamp();
  const expiredTimestamp = (
    parseInt(currentTimestamp) + commitingSeconds
  ).toString();

<<<<<<< HEAD
  const keyBytes = txExternalKey(tx);
  const txKey = buildKey(KEY_PREFIX_TX, keyBytes);
=======
  const lockedCellBytes = ccc.hexFrom(
    tx.inputs[tx.inputs.length - 1].previousOutput.toBytes(),
  );
  const txKey = buildKey(KEY_PREFIX_TX, lockedCellBytes);
  console.log("txKey", txKey);
>>>>>>> dbc4fae (chore: log params)
  const savedTxBytes = await dbConnection.get(txKey);

  const INVALID_CELL_ERROR = {
    error: {
      code: ERROR_CODE_INVALID_INPUT,
      message: "Locked cell is missing, invalid or expired!",
    },
  };
  console.log("savedTxBytes", savedTxBytes);

  if (savedTxBytes === null || savedTxBytes === undefined) {
    return INVALID_CELL_ERROR;
  }
  try {
    const savedTx = ccc.Transaction.fromBytes(savedTxBytes);
    if (!(await compareTx(tx, savedTx))) {
      Logger.error(`User provided a modified tx for ${savedTx.hash()}`);
      console.log("compareTx", compareTx(tx, savedTx));
      return INVALID_CELL_ERROR;
    }
  } catch (e) {
    Logger.error(`Parsing saved tx error: ${e}`);
    return INVALID_CELL_ERROR;
  }

  {
    // Commit all funder cells
    const funderScript = (await funder.getAddressObjSecp256k1()).script;

    for (const input of tx.inputs) {
      const inputCell = await input.getCell(funder.client);
      if (inputCell.cellOutput.lock.eq(funderScript)) {
        const commitResult = await (dbConnection as any).commitCell(
          KEY_LIVE_CELLS,
          KEY_LOCKED_CELLS,
          KEY_COMMITING_CELLS,
          txKey,
          ccc.hexFrom(input.previousOutput.toBytes()),
          currentTimestamp,
          expiredTimestamp,
        );
        if (!commitResult) {
          return INVALID_CELL_ERROR;
        }
      }
    }
  }

  const signedTxKey = buildKey(KEY_PREFIX_SIGNED_TX, keyBytes);
  await signerQueue.add("sign", {
    tx: ccc.hexFrom(tx.toBytes()),
    targetKey: signedTxKey,
    ex: commitingSeconds * 2,
  });

  await backOff(async () => {
    if ((await dbConnection.exists(signedTxKey)) < 1) {
      throw new Error("wait!");
    }
  });

  const signedTx = ccc.Transaction.fromBytes(
    (await dbConnection.get(signedTxKey))!,
  );

  try {
    const txHash = await funder.client.sendTransaction(signedTx);
    Logger.info(`Tx ${txHash} submitted to CKB!`);
  } catch (e) {
    const message = `Sending transaction ${signedTx.hash()} receives errors: ${e}`;
    Logger.error(message);
    await cancelAllCommitingCells(signedTx, funder, dbConnection);

    return {
      error: {
        code: ERROR_CODE_INVALID_INPUT,
        message,
      },
    };
  }
  await dbConnection.rpush(KEY_PENDING_TXS, ccc.hexFrom(signedTx.toBytes()));

  return {
    result: {
      transaction: buildTx(params.c, tx),
    },
  };
}

// Only witnesses belonging to users can be tweaked(mainly for signatures)
function compareTx(tx: ccc.Transaction, savedTx: ccc.Transaction): boolean {
  console.log("tx.hash()", tx.hash());
  console.log("savedTx.hash()", savedTx.hash());
  console.log("tx.witnesses.length", tx.witnesses.length);
  console.log("savedTx.witnesses.length", savedTx.witnesses.length);

  // Check hash first
  if (tx.hash() !== savedTx.hash()) {
    console.log("❌ Transaction hash mismatch!");
    console.log("tx inputs length:", tx.inputs.length);
    console.log("savedTx inputs length:", savedTx.inputs.length);
    console.log("tx outputs length:", tx.outputs.length);
    console.log("savedTx outputs length:", savedTx.outputs.length);

    // Compare inputs
    console.log("🔍 Comparing inputs:");
    for (let i = 0; i < Math.max(tx.inputs.length, savedTx.inputs.length); i++) {
      if (i < tx.inputs.length && i < savedTx.inputs.length) {
        const input1 = tx.inputs[i];
        const input2 = savedTx.inputs[i];
        console.log(`Input ${i}:`);
        console.log(`  tx: ${input1.previousOutput.txHash}:${input1.previousOutput.index}, since: ${input1.since}`);
        console.log(`  saved: ${input2.previousOutput.txHash}:${input2.previousOutput.index}, since: ${input2.since}`);
        console.log(`  match: ${input1.previousOutput.txHash === input2.previousOutput.txHash && input1.previousOutput.index === input2.previousOutput.index && input1.since === input2.since}`);
      }
    }

    // Compare outputs
    console.log("🔍 Comparing outputs:");
    for (let i = 0; i < Math.max(tx.outputs.length, savedTx.outputs.length); i++) {
      if (i < tx.outputs.length && i < savedTx.outputs.length) {
        const output1 = tx.outputs[i];
        const output2 = savedTx.outputs[i];
        console.log(`Output ${i}:`);
        console.log(`  tx capacity: ${output1.capacity}`);
        console.log(`  saved capacity: ${output2.capacity}`);
        console.log(`  capacity match: ${output1.capacity === output2.capacity}`);

        // Compare lock scripts
        console.log(`  tx lock: ${output1.lock?.codeHash}:${output1.lock?.hashType}:${output1.lock?.args}`);
        console.log(`  saved lock: ${output2.lock?.codeHash}:${output2.lock?.hashType}:${output2.lock?.args}`);

        // Compare type scripts
        console.log(`  tx type: ${output1.type?.codeHash}:${output1.type?.hashType}:${output1.type?.args}`);
        console.log(`  saved type: ${output2.type?.codeHash}:${output2.type?.hashType}:${output2.type?.args}`);
      }
    }

    // Compare outputs data
    console.log("🔍 Comparing outputs data:");
    for (let i = 0; i < Math.max(tx.outputsData.length, savedTx.outputsData.length); i++) {
      if (i < tx.outputsData.length && i < savedTx.outputsData.length) {
        console.log(`OutputData ${i}:`);
        console.log(`  tx: ${tx.outputsData[i]}`);
        console.log(`  saved: ${savedTx.outputsData[i]}`);
        console.log(`  match: ${tx.outputsData[i] === savedTx.outputsData[i]}`);
      }
    }

    // Compare cellDeps
    console.log("🔍 Comparing cellDeps:");
    console.log("tx cellDeps length:", tx.cellDeps.length);
    console.log("savedTx cellDeps length:", savedTx.cellDeps.length);
    for (let i = 0; i < Math.max(tx.cellDeps.length, savedTx.cellDeps.length); i++) {
      if (i < tx.cellDeps.length && i < savedTx.cellDeps.length) {
        const cellDep1 = tx.cellDeps[i];
        const cellDep2 = savedTx.cellDeps[i];
        console.log(`CellDep ${i}:`);
        console.log(`  tx: ${cellDep1.outPoint.txHash}:${cellDep1.outPoint.index}, depType: ${cellDep1.depType}`);
        console.log(`  saved: ${cellDep2.outPoint.txHash}:${cellDep2.outPoint.index}, depType: ${cellDep2.depType}`);
        const match = cellDep1.outPoint.txHash === cellDep2.outPoint.txHash &&
                      cellDep1.outPoint.index === cellDep2.outPoint.index &&
                      cellDep1.depType === cellDep2.depType;
        console.log(`  match: ${match}`);
      }
    }

    // Compare headerDeps
    console.log("🔍 Comparing headerDeps:");
    console.log("tx headerDeps length:", tx.headerDeps.length);
    console.log("savedTx headerDeps length:", savedTx.headerDeps.length);
    for (let i = 0; i < Math.max(tx.headerDeps.length, savedTx.headerDeps.length); i++) {
      if (i < tx.headerDeps.length && i < savedTx.headerDeps.length) {
        console.log(`HeaderDep ${i}:`);
        console.log(`  tx: ${tx.headerDeps[i]}`);
        console.log(`  saved: ${savedTx.headerDeps[i]}`);
        console.log(`  match: ${tx.headerDeps[i] === savedTx.headerDeps[i]}`);
      }
    }

    // Compare version
    console.log("🔍 Comparing version:");
    console.log("tx version:", tx.version);
    console.log("savedTx version:", savedTx.version);
    console.log("version match:", tx.version === savedTx.version);

    return false;
  }

  // Check witnesses length
  if (tx.witnesses.length !== savedTx.witnesses.length) {
    console.log("❌ Witnesses length mismatch!");
    return false;
  }

  // Check last witness
  const lastWitnessMatch = tx.witnesses[tx.witnesses.length - 1] === savedTx.witnesses[savedTx.witnesses.length - 1];
  if (!lastWitnessMatch) {
    console.log("❌ Last witness mismatch!");
    console.log("tx last witness:", tx.witnesses[tx.witnesses.length - 1]);
    console.log("savedTx last witness:", savedTx.witnesses[savedTx.witnesses.length - 1]);
    return false;
  }

  // Check other witnesses lengths
  // for (let i = 0; i < tx.witnesses.length - 1; i++) {
  //   if (tx.witnesses[i].length !== savedTx.witnesses[i].length) {
  //     console.log(`❌ Witness ${i} length mismatch!`);
  //     console.log(`tx witness ${i} length:`, tx.witnesses[i].length);
  //     console.log(`savedTx witness ${i} length:`, savedTx.witnesses[i].length);
  //     return false;
  //   }
  // }

  console.log("✅ All checks passed!");
  return true;
}
