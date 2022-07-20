package lib

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/golang/glog"
	"github.com/google/uuid"
	"time"
)

type SQSQueue struct {
	sqsClient *sqs.Client
	queueUrl  *string
	params    *DeSoParams
}

type TransactionMessage struct {
	TransactionType    string
	TransactionHashHex string
	TransactionData    interface{}
}

func NewSQSQueue(client *sqs.Client, queueUrl string, params *DeSoParams) *SQSQueue {
	newSqsQueue := SQSQueue{}
	newSqsQueue.sqsClient = client
	newSqsQueue.queueUrl = &queueUrl
	newSqsQueue.params = params
	return &newSqsQueue
}

type PrivateMessageTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	RecipientPublicKey             string
	SenderMessagingGroupKeyName    string
	RecipientMessagingGroupKeyName string
	EncryptedText                  string
}

type SubmitPostTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	PostHashToModify               string
	ParentStakeID                  string
	Body                           string
	CreatorBasisPoints             uint64
	StakeMultipleBasisPoints       uint64
	IsHidden                       bool
}

type LikeTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	LikedPostHashHex               string
	IsUnlike                       bool
}

type FollowTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	FollowedPublicKey              string
	IsUnfollow                     bool
}

type BasicTransferTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	DiamondLevel                   int64
	PostHashHex                    string
}

type CreatorCoinTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	ProfilePublicKey               string
	OperationType                  CreatorCoinOperationType
	BitCloutToSellNanos            uint64
	CreatorCoinToSellNanos         uint64
	BitCloutToAddNanos             uint64
	MinBitCloutExpectedNanos       uint64
	MinCreatorCoinExpectedNanos    uint64
}

type CreatorCoinTransferTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	ProfilePublicKey               string
	CreatorCoinToTransferNanos     uint64
	ReceiverPublicKey              string
}

// Filter unnecessary fields and send txn to the configured SQS Queue
func (sqsQueue *SQSQueue) SendSQSTxnMessage(mempoolTxn *MempoolTx) {
	txn := mempoolTxn.Tx
	var transactionData interface{}
	switch txn.TxnMeta.GetTxnType() {
	case TxnTypeSubmitPost:
		transactionData = makeSubmitPostTransactionData(mempoolTxn)
	case TxnTypeLike:
		transactionData = makeLikeTransactionData(mempoolTxn)
	case TxnTypeFollow:
		transactionData = makeFollowTransactionData(mempoolTxn)
	case TxnTypeBasicTransfer:
		transactionData = makeBasicTransferTransactionData(mempoolTxn)
	case TxnTypeCreatorCoin:
		transactionData = makeCreatorCoinTransactionData(mempoolTxn, sqsQueue.params)
	case TxnTypeCreatorCoinTransfer:
		transactionData = makeCreatorCoinTransferTransactionData(mempoolTxn, sqsQueue.params)
	case TxnTypePrivateMessage:
		transactionData = makePrivateMessageTransactionData(mempoolTxn, sqsQueue.params)
	default:
		return
	}

	transactionMessage := TransactionMessage{
		TransactionType:    txn.TxnMeta.GetTxnType().String(),
		TransactionHashHex: hex.EncodeToString(txn.Hash()[:]),
		TransactionData:    transactionData,
	}

	res, err := json.Marshal(transactionMessage)
	if err != nil {
		glog.Errorf("SendSQSTxnMessage: Error marshaling transaction JSON : %v", err)
	}

	messageAttributes := make(map[string]types.MessageAttributeValue)
	messageAttributes["messageId"] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(uuid.NewString()),
	}

	sendMessageInput := &sqs.SendMessageInput{
		DelaySeconds:      0,
		MessageBody:       aws.String(string(res)),
		MessageAttributes: messageAttributes,
		QueueUrl:          sqsQueue.queueUrl,
	}
	_, err = sqsQueue.sqsClient.SendMessage(context.TODO(), sendMessageInput)
	if err != nil {
		glog.Infof("SendSQSTxnMessage hash hex : %v", transactionMessage.TransactionHashHex)
		glog.Infof("SendSQSTxnMessage type : %v", transactionMessage.TransactionType)
		glog.Infof("SendSQSTxnMessage input : %v", sendMessageInput)
		glog.Errorf("SendSQSTxnMessage: Error sending sqs message : %v", err)
	}
}

func makePrivateMessageTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *PrivateMessageTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*PrivateMessageMetadata)
	extraData := mempoolTxn.Tx.ExtraData
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys

	senderMessagingGroupKeyNameBytes, foundSenderGroupKeyName := extraData[SenderMessagingGroupKeyName]
	senderMessagingGroupKeyName := ""
	if foundSenderGroupKeyName {
		senderMessagingGroupKeyName = string(senderMessagingGroupKeyNameBytes)
	}

	recipientMessagingGroupKeyNameBytes, foundRecipientGroupKeyName := extraData[RecipientMessagingGroupKeyName]
	recipientMessagingGroupKeyName := ""
	if foundRecipientGroupKeyName {
		recipientMessagingGroupKeyName = string(recipientMessagingGroupKeyNameBytes)
	}

	return &PrivateMessageTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		RecipientPublicKey:             PkToString(metadata.RecipientPublicKey, params),
		SenderMessagingGroupKeyName:    senderMessagingGroupKeyName,
		RecipientMessagingGroupKeyName: recipientMessagingGroupKeyName,
		TimestampNanos:                 metadata.TimestampNanos,
		EncryptedText:                  hex.EncodeToString(metadata.EncryptedText),
	}
}

func makeSubmitPostTransactionData(mempoolTxn *MempoolTx) *SubmitPostTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*SubmitPostMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &SubmitPostTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		PostHashToModify:               hex.EncodeToString(metadata.PostHashToModify),
		ParentStakeID:                  hex.EncodeToString(metadata.ParentStakeID),
		Body:                           string(metadata.Body),
		CreatorBasisPoints:             metadata.CreatorBasisPoints,
		StakeMultipleBasisPoints:       metadata.StakeMultipleBasisPoints,
		TimestampNanos:                 metadata.TimestampNanos,
		IsHidden:                       metadata.IsHidden,
	}
}

func makeLikeTransactionData(mempoolTxn *MempoolTx) *LikeTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*LikeMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &LikeTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		LikedPostHashHex:               hex.EncodeToString(metadata.LikedPostHash[:]),
		IsUnlike:                       metadata.IsUnlike,
	}
}

func makeFollowTransactionData(mempoolTxn *MempoolTx) *FollowTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*FollowMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &FollowTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		FollowedPublicKey:              PkToString(metadata.FollowedPublicKey),
		IsUnfollow:                     metadata.IsUnfollow,
	}
}

func makeBasicTransferTransactionData(mempoolTxn *MempoolTx) *BasicTransferTransactionData {
	metadata := mempoolTxn.TxMeta.BasicTransferTxindexMetadata
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	// TODO figure out of if any other basic transfers besides diamonds are relevant to us
	return &BasicTransferTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		DiamondLevel:                   metadata.DiamondLevel,
		PostHashHex:                    metadata.PostHashHex,
	}
}

func makeCreatorCoinTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *CreatorCoinTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*CreatorCoinMetadataa)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &CreatorCoinTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               PkToString(metadata.ProfilePublicKey, params),
		OperationType:                  metadata.OperationType,
		BitCloutToSellNanos:            metadata.DeSoToSellNanos,
		CreatorCoinToSellNanos:         metadata.CreatorCoinToSellNanos,
		BitCloutToAddNanos:             metadata.DeSoToAddNanos,
		MinBitCloutExpectedNanos:       metadata.MinDeSoExpectedNanos,
		MinCreatorCoinExpectedNanos:    metadata.MinCreatorCoinExpectedNanos,
	}
}

func makeCreatorCoinTransferTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *CreatorCoinTransferTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*CreatorCoinTransferMetadataa)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &CreatorCoinTransferTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               PkToString(metadata.ProfilePublicKey, params),
		CreatorCoinToTransferNanos:     metadata.CreatorCoinToTransferNanos,
		ReceiverPublicKey:              PkToString(metadata.ReceiverPublicKey, params),
	}
}
