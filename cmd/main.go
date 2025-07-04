package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/mugiliam/common/logtrace"
	"github.com/mugiliam/hatchcatalogsrv/internal/common"
	"github.com/mugiliam/hatchcatalogsrv/internal/config"
	"github.com/mugiliam/hatchcatalogsrv/internal/server"
	"github.com/rs/zerolog/log"
)

func init() {
	logtrace.InitLogger()
}

type cmdoptions struct {
	configFile *string
}

func main() {

	slog := log.With().Str("state", "init").Logger()
	// Parse command line flags
	opt := parseFlags()

	slog.Info().Str("config_file", *opt.configFile).Msg("loading config file")
	// load config file
	if err := config.LoadConfig(*opt.configFile); err != nil {
		slog.Error().Str("config_file", *opt.configFile).Err(err).Msg("unable to load config file")
		os.Exit(1)
	}
	if config.Config().ServerPort == "" {
		slog.Error().Msg("server port not defined")
		os.Exit(1)
	}
	s, err := server.CreateNewServer()
	if err != nil {
		slog.Error().Err(err).Msg("Unable to create server")
	}
	s.MountHandlers()
	http.ListenAndServe(":"+config.Config().ServerPort, s.Router)
}

func parseFlags() cmdoptions {
	var opt cmdoptions
	opt.configFile = flag.String("config", common.DefaultConfigFile, "Path to the config file")
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: %s [options]\n\n", os.Args[0])
		fmt.Println("Options:")
		flag.PrintDefaults()
	}
	flag.Parse()
	return opt
}
