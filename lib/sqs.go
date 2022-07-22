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
	TransactionType    string      `json:"transactionType"`
	TransactionHashHex string      `json:"transactionHashHex"`
	TransactionData    interface{} `json:"transactionData"`
}

func NewSQSQueue(client *sqs.Client, queueUrl string, params *DeSoParams) *SQSQueue {
	newSqsQueue := SQSQueue{}
	newSqsQueue.sqsClient = client
	newSqsQueue.queueUrl = &queueUrl
	newSqsQueue.params = params
	return &newSqsQueue
}

type AffectedPublicKeyForJson struct {
	PublicKeyBase58Check string `json:"publicKeyBase58Check"`
	Metadata             string `json:"metadata"`
}

type PrivateMessageTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	RecipientPublicKey             string                      `json:"recipientPublicKey"`
	SenderMessagingGroupKeyName    string                      `json:"senderMessagingGroupKeyName"`
	RecipientMessagingGroupKeyName string                      `json:"recipientMessagingGroupKeyName"`
	EncryptedText                  string                      `json:"encryptedText"`
}

type SubmitPostTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	PostHashToModify               string                      `json:"postHashToModify"`
	ParentStakeID                  string                      `json:"parentStakeID"`
	Body                           string                      `json:"body"`
	CreatorBasisPoints             uint64                      `json:"creatorBasisPoints"`
	StakeMultipleBasisPoints       uint64                      `json:"stakeMultipleBasisPoints"`
	IsHidden                       bool                        `json:"isHidden"`
}

type LikeTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	LikedPostHashHex               string                      `json:"likedPostHashHex"`
	IsUnlike                       bool                        `json:"isUnlike"`
}

type FollowTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	FollowedPublicKey              string                      `json:"followedPublicKey"`
	IsUnfollow                     bool                        `json:"isUnfollow"`
}

type BasicTransferTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	DiamondLevel                   int64                       `json:"diamondLevel"`
	PostHashHex                    string                      `json:"postHashHex"`
}

type CreatorCoinTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	ProfilePublicKey               string                      `json:"profilePublicKey"`
	OperationType                  CreatorCoinOperationType    `json:"operationType"`
	DeSoToSellNanos                uint64                      `json:"deSoToSellNanos"`
	CreatorCoinToSellNanos         uint64                      `json:"creatorCoinToSellNanos"`
	DeSoToAddNanos                 uint64                      `json:"deSoToAddNanos"`
	MinDeSoExpectedNanos           uint64                      `json:"minDeSoExpectedNanos"`
	MinCreatorCoinExpectedNanos    uint64                      `json:"minCreatorCoinExpectedNanos"`
}

type CreatorCoinTransferTransactionData struct {
	AffectedPublicKeys             []*AffectedPublicKeyForJson `json:"affectedPublicKeys"`
	TimestampNanos                 uint64                      `json:"timestampNanos"`
	TransactorPublicKeyBase58Check string                      `json:"transactorPublicKeyBase58Check"`
	ProfilePublicKey               string                      `json:"profilePublicKey"`
	CreatorCoinToTransferNanos     uint64                      `json:"creatorCoinToTransferNanos"`
	ReceiverPublicKey              string                      `json:"receiverPublicKey"`
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
		transactionData = makeFollowTransactionData(mempoolTxn, sqsQueue.params)
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
	stringDataType := aws.String("String")
	messageAttributes["contentType"] = types.MessageAttributeValue{
		DataType:    stringDataType,
		StringValue: aws.String("application/json"),
	}
	messageAttributes["messageId"] = types.MessageAttributeValue{
		DataType:    stringDataType,
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

func getJson(affectedPublicKeys []*AffectedPublicKey) []*AffectedPublicKeyForJson {
	affectedPublicKeysForJson := make([]*AffectedPublicKeyForJson, len(affectedPublicKeys))
	for index := range affectedPublicKeys {
		affectedPublicKeysForJson[index] = (*AffectedPublicKeyForJson)(affectedPublicKeys[index])
	}
	return affectedPublicKeysForJson
}

func makePrivateMessageTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *PrivateMessageTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*PrivateMessageMetadata)
	extraData := mempoolTxn.Tx.ExtraData
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)

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
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
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
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
	return &LikeTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		LikedPostHashHex:               hex.EncodeToString(metadata.LikedPostHash[:]),
		IsUnlike:                       metadata.IsUnlike,
	}
}

func makeFollowTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *FollowTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*FollowMetadata)
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
	return &FollowTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		FollowedPublicKey:              PkToString(metadata.FollowedPublicKey, params),
		IsUnfollow:                     metadata.IsUnfollow,
	}
}

func makeBasicTransferTransactionData(mempoolTxn *MempoolTx) *BasicTransferTransactionData {
	metadata := mempoolTxn.TxMeta.BasicTransferTxindexMetadata
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
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
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
	return &CreatorCoinTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               PkToString(metadata.ProfilePublicKey, params),
		OperationType:                  metadata.OperationType,
		DeSoToSellNanos:                metadata.DeSoToSellNanos,
		CreatorCoinToSellNanos:         metadata.CreatorCoinToSellNanos,
		DeSoToAddNanos:                 metadata.DeSoToAddNanos,
		MinDeSoExpectedNanos:           metadata.MinDeSoExpectedNanos,
		MinCreatorCoinExpectedNanos:    metadata.MinCreatorCoinExpectedNanos,
	}
}

func makeCreatorCoinTransferTransactionData(mempoolTxn *MempoolTx, params *DeSoParams) *CreatorCoinTransferTransactionData {
	metadata := mempoolTxn.Tx.TxnMeta.(*CreatorCoinTransferMetadataa)
	affectedPublicKeys := getJson(mempoolTxn.TxMeta.AffectedPublicKeys)
	return &CreatorCoinTransferTransactionData{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               PkToString(metadata.ProfilePublicKey, params),
		CreatorCoinToTransferNanos:     metadata.CreatorCoinToTransferNanos,
		ReceiverPublicKey:              PkToString(metadata.ReceiverPublicKey, params),
	}
}
