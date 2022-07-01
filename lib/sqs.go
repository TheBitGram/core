package lib

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/golang/glog"
)

type SQSQueue struct {
	sqsClient *sqs.Client
	queueUrl  *string
}

type SqsInput struct {
	TransactionType    string
	TransactionHashHex string
	TransactionData    BitCloutNotification
}

type BitCloutNotification interface {
}

func NewSQSQueue(client *sqs.Client, queueUrl string) *SQSQueue {
	newSqsQueue := SQSQueue{}
	newSqsQueue.sqsClient = client
	newSqsQueue.queueUrl = &queueUrl
	return &newSqsQueue
}

type PrivateMessageTransaction struct {
	AffectedPublicKeys []*AffectedPublicKey
	TimestampNanos uint64
	TransactorPublicKeyBase58Check string
	EncryptedText []byte
}

type SubmitPostTransaction struct {
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

type LikeTransaction struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	LikedPostHashHex               string
	IsUnlike                       bool
}

type FollowTransaction struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	FollowedPublicKey              string
	IsUnfollow                     bool
}

type BasicTransferTransaction struct {
	AffectedPublicKeys             []*AffectedPublicKey
	TimestampNanos                 uint64
	TransactorPublicKeyBase58Check string
	DiamondLevel                   int64
	PostHashHex                    string
}

type CreatorCoinTransaction struct {
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

type CreatorCoinTransferTransaction struct {
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
	var transactionData BitCloutNotification
	switch txn.TxnMeta.GetTxnType() {
	case TxnTypeSubmitPost:
		transactionData = makeSubmitPostNotification(mempoolTxn)
	case TxnTypeLike:
		transactionData = makeLikeNotification(mempoolTxn)
	case TxnTypeFollow:
		transactionData = makeFollowNotification(mempoolTxn)
	case TxnTypeBasicTransfer:
		transactionData = makeBasicTransferNotification(mempoolTxn)
	case TxnTypeCreatorCoin:
		transactionData = makeCreatorCoinNotification(mempoolTxn)
	case TxnTypeCreatorCoinTransfer:
		transactionData = makeCreatorCoinTransferNotification(mempoolTxn)
	case TxnTypePrivateMessage:
		transactionData = makePrivateMessageNotification(mempoolTxn)
	default:
		return
	}

	sqsInput := SqsInput{
		TransactionType:    txn.TxnMeta.GetTxnType().String(),
		TransactionHashHex: hex.EncodeToString(txn.Hash()[:]),
		TransactionData: transactionData,
	}

	res, err := json.Marshal(sqsInput)
	if err != nil {
		glog.Errorf("SendSQSTxnMessage: Error marshaling transaction JSON : %v", err)
	}

	sendMessageInput := &sqs.SendMessageInput{
		DelaySeconds: 0,
		MessageBody:  aws.String(string(res)),
		QueueUrl:     sqsQueue.queueUrl,
	}
	_, err = sqsQueue.sqsClient.SendMessage(context.TODO(), sendMessageInput)
	if err != nil {
		glog.Infof("SendSQSTxnMessage hash hex : %v", sqsInput.TransactionHashHex)
		glog.Infof("SendSQSTxnMessage type : %v", sqsInput.TransactionType)
		glog.Infof("SendSQSTxnMessage input : %v", sendMessageInput)
		glog.Errorf("SendSQSTxnMessage: Error sending sqs message : %v", err)
	}
}

func makePrivateMessageNotification(mempoolTxn *MempoolTx) (*PrivateMessageTransaction){
	metadata := mempoolTxn.Tx.TxnMeta.(*PrivateMessageMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &PrivateMessageTransaction {
		AffectedPublicKeys: 		    affectedPublicKeys,
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		TimestampNanos: 				metadata.TimestampNanos,
		EncryptedText: 					metadata.EncryptedText,
	}
}

func makeSubmitPostNotification(mempoolTxn *MempoolTx) (*SubmitPostTransaction){
	metadata := mempoolTxn.Tx.TxnMeta.(*SubmitPostMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &SubmitPostNotification{
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

func makeLikeNotification(mempoolTxn *MempoolTx) (*LikeTransaction) {
	metadata := mempoolTxn.Tx.TxnMeta.(*LikeMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &LikeNotification{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		LikedPostHashHex:               hex.EncodeToString([]byte(metadata.LikedPostHash[:])),
		IsUnlike:                       metadata.IsUnlike,
	}
}

func makeFollowNotification(mempoolTxn *MempoolTx) (*FollowTransaction) {
	metadata := mempoolTxn.Tx.TxnMeta.(*FollowMetadata)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &FollowNotification{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		FollowedPublicKey:              hex.EncodeToString(metadata.FollowedPublicKey),
		IsUnfollow:                     metadata.IsUnfollow,
	}
}

func makeBasicTransferNotification(mempoolTxn *MempoolTx) (*BasicTransferTransaction) {
	metadata := mempoolTxn.TxMeta.BasicTransferTxindexMetadata
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	// TODO figure out of if any other basic transfers besides diamonds are relevant to us
	return &BasicTransferNotification{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		DiamondLevel:                   metadata.DiamondLevel,
		PostHashHex:                    metadata.PostHashHex,
	}
}

func makeCreatorCoinNotification(mempoolTxn *MempoolTx) (*CreatorCoinTransaction) {
	metadata := mempoolTxn.Tx.TxnMeta.(*CreatorCoinMetadataa)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &CreatorCoinNotification{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               hex.EncodeToString(metadata.ProfilePublicKey),
		OperationType:                  metadata.OperationType,
		BitCloutToSellNanos:            metadata.DeSoToSellNanos,
		CreatorCoinToSellNanos:         metadata.CreatorCoinToSellNanos,
		BitCloutToAddNanos:             metadata.DeSoToAddNanos,
		MinBitCloutExpectedNanos:       metadata.MinDeSoExpectedNanos,
		MinCreatorCoinExpectedNanos:    metadata.MinCreatorCoinExpectedNanos,
	}
}

func makeCreatorCoinTransferNotification(mempoolTxn *MempoolTx) (*CreatorCoinTransferTransaction) {
	metadata := mempoolTxn.Tx.TxnMeta.(*CreatorCoinTransferMetadataa)
	affectedPublicKeys := mempoolTxn.TxMeta.AffectedPublicKeys
	return &CreatorCoinTransferNotification{
		AffectedPublicKeys:             affectedPublicKeys,
		TimestampNanos:                 uint64(time.Now().UnixNano()),
		TransactorPublicKeyBase58Check: mempoolTxn.TxMeta.TransactorPublicKeyBase58Check,
		ProfilePublicKey:               hex.EncodeToString(metadata.ProfilePublicKey),
		CreatorCoinToTransferNanos:     metadata.CreatorCoinToTransferNanos,
		ReceiverPublicKey:              hex.EncodeToString(metadata.ReceiverPublicKey),
	}
}
