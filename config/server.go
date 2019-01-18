package config

type ServerCfg struct {
	Bind        string
	IsMaster    bool
	ReplicateTo string
}
