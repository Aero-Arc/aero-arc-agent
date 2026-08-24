package wal

type DeliveryStatus int

const (
	DeliveryStatusWritten     DeliveryStatus = 0
	DeliveryStatusPending     DeliveryStatus = 1
	DeliveryStatusDelivered   DeliveryStatus = 2
	DeliveryStatusQuarantined DeliveryStatus = 3
)
