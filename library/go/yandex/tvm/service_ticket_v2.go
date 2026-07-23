package tvm

type CheckedTickets struct {
	User    CheckedUserTicket
	Service CheckedServiceTicket
}

type GetServiceTicketV2Options struct {
	Src string
}

type GetServiceTicketV2Option func(*GetServiceTicketV2Options)

func WithSrcOverride(src string) GetServiceTicketV2Option {
	return func(opts *GetServiceTicketV2Options) {
		opts.Src = src
	}
}
