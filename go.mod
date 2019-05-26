module github.com/kubedb/mariadb

go 1.12

require (
	github.com/appscode/go v0.0.0-20190523031839-1468ee3a76e8
	github.com/appscode/osm v0.11.0 // indirect
	github.com/codeskyblue/go-sh v0.0.0-20190412065543-76bd3d59ff27
	github.com/coreos/go-semver v0.3.0
	github.com/coreos/prometheus-operator v0.29.0
	github.com/denisenkom/go-mssqldb v0.0.0-20190423183735-731ef375ac02 // indirect
	github.com/dnaeon/go-vcr v1.0.1 // indirect
	github.com/fatih/structs v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/go-xorm/builder v0.0.0-20190422082613-0c156dfdb061 // indirect
	github.com/go-xorm/core v0.6.0 // indirect
	github.com/go-xorm/xorm v0.7.0
	github.com/google/uuid v1.1.1
	github.com/graymeta/stow v0.0.0-00010101000000-000000000000
	github.com/kubedb/apimachinery v0.0.0-20190526014453-48e4bab67179
	github.com/lib/pq v1.1.1 // indirect
	github.com/mattn/go-sqlite3 v1.10.0 // indirect
	github.com/ncw/swift v1.0.47 // indirect
	github.com/onsi/ginkgo v1.8.0
	github.com/onsi/gomega v1.5.0
	github.com/pkg/errors v0.8.1
	github.com/spf13/cobra v0.0.4
	github.com/spf13/pflag v1.0.3
	github.com/ziutek/mymysql v1.5.4 // indirect
	golang.org/x/crypto v0.0.0-20190513172903-22d7a77e9e5f // indirect
	golang.org/x/net v0.0.0-20190514140710-3ec191127204 // indirect
	golang.org/x/sys v0.0.0-20190516110030-61b9204099cb // indirect
	k8s.io/api v0.0.0-20190503110853-61630f889b3c
	k8s.io/apiextensions-apiserver v0.0.0-20190508184259-7784d62bc471
	k8s.io/apimachinery v0.0.0-20190508063446-a3da69d3723c
	k8s.io/apiserver v0.0.0-20190508183956-3a0abf14e58a
	k8s.io/cli-runtime v0.0.0-20190508184404-b26560c459bd // indirect
	k8s.io/client-go v11.0.0+incompatible
	k8s.io/kube-aggregator v0.0.0-20190508104018-6d3d96b06d29
	kmodules.xyz/client-go v0.0.0-20190524133821-9c8a87771aea
	kmodules.xyz/custom-resources v0.0.0-20190508103408-464e8324c3ec
	kmodules.xyz/monitoring-agent-api v0.0.0-20190508125842-489150794b9b
	kmodules.xyz/objectstore-api v0.0.0-20190506085934-94c81c8acca9
	kmodules.xyz/offshoot-api v0.0.0-20190508142450-1c69d50f3c1c
	kmodules.xyz/webhook-runtime v0.0.0-20190508093950-b721b4eba5e5
)

replace (
	github.com/graymeta/stow => github.com/appscode/stow v0.0.0-20190506085026-ca5baa008ea3
	gopkg.in/robfig/cron.v2 => github.com/appscode/cron v0.0.0-20170717094345-ca60c6d796d4
	k8s.io/api => k8s.io/api v0.0.0-20190313235455-40a48860b5ab
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20190315093550-53c4693659ed
	k8s.io/apimachinery => github.com/kmodules/apimachinery v0.0.0-20190508045248-a52a97a7a2bf
	k8s.io/apiserver => github.com/kmodules/apiserver v0.0.0-20190508082252-8397d761d4b5
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20190314001948-2899ed30580f
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20190314002645-c892ea32361a
	k8s.io/component-base => k8s.io/component-base v0.0.0-20190314000054-4a91899592f4
	k8s.io/klog => k8s.io/klog v0.3.0
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.0.0-20190314000639-da8327669ac5
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
	k8s.io/metrics => k8s.io/metrics v0.0.0-20190314001731-1bd6a4002213
	k8s.io/utils => k8s.io/utils v0.0.0-20190221042446-c2654d5206da
)
